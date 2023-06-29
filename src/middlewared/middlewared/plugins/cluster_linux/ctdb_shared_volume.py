import errno
import json
from pathlib import Path
from glustercli.cli import volume

from middlewared.service import Service, CallError, job
from middlewared.plugins.cluster_linux.utils import CTDBConfig
from middlewared.plugins.gluster_linux.utils import GlusterConfig


MOUNT_UMOUNT_LOCK = CTDBConfig.MOUNT_UMOUNT_LOCK.value
CRE_OR_DEL_LOCK = CTDBConfig.CRE_OR_DEL_LOCK.value
LEGACY_CTDB_VOL_NAME = CTDBConfig.CTDB_VOL_NAME.value
CTDB_LOCAL_MOUNT = CTDBConfig.CTDB_LOCAL_MOUNT.value
CTDB_VOL_INFO_FILE = CTDBConfig.CTDB_VOL_INFO_FILE.value
METADATA_VOL_FILE = GlusterConfig.METADATA_VOLUME.value


class CtdbSharedVolumeService(Service):

    class Config:
        namespace = 'ctdb.shared.volume'
        private = True

    def get_metadata_config(self):
        try:
            with open(METADATA_VOL_FILE, 'r') as f:
                vol_name = f.read()

            return {
                'volume': vol,
                'metadata_dir': '.truenas_metadata'
            }
        except FileNotFoundError:
            pass

        vol_names = [x['name'] for x in self.middleware.call_sync('gluster.volume.list')]
        if LEGACY_CTDB_VOL_NAME in vol_names:
            try:
                self.middleware.call_sync('gluster.filesystem.lookup', {
                    'volume_name': LEGACY_CTDB_VOL_NAME,
                    'path': '.DEPRECATED' 
                })
            except Exception:
                self.logger.debug("failed to look up sentinel", exc_info=True)
                return {
                    'volume': LEGACY_CTDB_VOL_NAME,
                    'metadata_dir': '/'
                }

        for vol in vol_names:
            if vol == LEGACY_CTDB_VOL_NAME:
                continue

            try:
                self.middleware.call_sync('gluster.filesystem.lookup', {
                    'volume_name': vol,
                    'path': '.truenas_metadata' 
                })
            except Exception:
                self.logger.debug("no metadata dir on %s", vol, exc_info=True) 
                continue

            return {
                'volume': vol,
                'metadata_dir': '.truenas_metadata'
            }

        raise CallError("No metadata dir configured")

    def generate_info(self):
        conf = self.get_metdata_config()

        volume = conf['volume'] 
        volume_mp = Path(CTDBConfig.LOCAL_MOUNT_BASE.value, volume)
        metadata_dir = conf['metadata_dir'] 
        glfs_uuid = self.middleware.call_sync('gluster.filesystem.lookup', {
            'volume_name': volume,
            'path': metadata_dir
        })['uuid']
        data = {
            'volume_name': volume,
            'volume_mountpoint': str(volume_mp),
            'path': metadata_dir,
            'mountpoint': str(Path(f'{volume_mp}/{metadata_dir}')),
            'uuid': glfs_uuid
        }
        with open(f'{CTDB_VOL_INFO_FILE}.tmp', "w") as f:
            f.write(json.dumps(data))

        os.rename(f'{CTDB_VOL_INFO_FILE}.tmp', CTDB_VOL_INFO_FILE)
        return data

    def config(self):
        info_file = Path(CTDB_VOL_INFO_FILE)
        try:
            return json.loads(info_file.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return self.generate_info()

    def update(self, data):
        info = self.middleware.call_sync('gluster.volume.exists_and_started', data['name'])
        if not info['exists']:
            raise CallError(
                f'{data["name"]}: volume does not exist', errno.ENOENT 
            )

        if not info['started']:
            self.middleware.call_sync('gluster.volume.start', {'name': data['name']})

        if self.middleware.call_sync('service.started', 'ctdb'):
            raise CallError(
                'Updates to TrueNAS clustered metadata volume are not '
                'permitted when the ctdb service is started'
            )

        with open(f'{METADATA_VOL_FILE}.tmp', 'w') as f:
            vol_name = f.write(data['name'])
            f.flush()
            os.fsync(f.fileno())

        os.rename(f'{METADATA_VOL_FILE}.tmp', METADATA_VOL_FILE)
        return self.generate_info()

    @job(lock=CRE_OR_DEL_LOCK)
    def migrate(self, job, data):
        /* prior to migration must stop ctdb daemon */
        data = {'event': 'CTDB_STOP', 'name': CTDB_VOL_NAME, 'forward': True}
        self.middleware.call_sync('gluster.localevents.send', data)



    async def validate(self):
        filters = [('id', '=', CTDB_VOL_NAME)]
        ctdb = await self.middleware.call('gluster.volume.query', filters)
        if not ctdb:
            # it's expected that ctdb shared volume exists when
            # calling this method
            raise CallError(f'{CTDB_VOL_NAME} does not exist', errno.ENOENT)

        for i in ctdb:
            err_msg = f'A volume named "{CTDB_VOL_NAME}" already exists '
            if i['type'] != 'REPLICATE':
                err_msg += (
                    'but is not a "REPLICATE" type volume. '
                    'Please delete or rename this volume and try again.'
                )
                raise CallError(err_msg)
            elif i['replica'] < 3 or i['num_bricks'] < 3:
                err_msg += (
                    'but is configured in a way that '
                    'could cause data corruption. Please delete '
                    'or rename this volume and try again.'
                )
                raise CallError(err_msg)

    @job(lock=CRE_OR_DEL_LOCK)
    async def create(self, job):
        """
        Create and mount the shared volume to be used
        by ctdb daemon.
        """
        # check if ctdb shared volume already exists and started
        info = await self.middleware.call('gluster.volume.exists_and_started', CTDB_VOL_NAME)
        if not info['exists']:
            # get the peers in the TSP
            peers = await self.middleware.call('gluster.peer.query')
            if not peers:
                raise CallError('No peers detected')

            # shared storage volume requires 3 nodes, minimally, to
            # prevent the dreaded split-brain
            con_peers = [i['hostname'] for i in peers if i['connected'] == 'Connected']
            if len(con_peers) < 3:
                raise CallError(
                    '3 peers must be present and connected before the ctdb '
                    'shared volume can be created.'
                )

            # get the system dataset location
            ctdb_sysds_path = (await self.middleware.call('systemdataset.config'))['path']
            if not ctdb_sysds_path:
                raise CallError("System dataset is not properly mounted")

            ctdb_sysds_path = str(Path(ctdb_sysds_path).joinpath(CTDB_VOL_NAME))

            bricks = []
            for i in con_peers:
                bricks.append(i + ':' + ctdb_sysds_path)

            options = {'args': (CTDB_VOL_NAME, bricks,)}
            options['kwargs'] = {'replica': len(con_peers), 'force': True}
            await self.middleware.call('gluster.method.run', volume.create, options)

        # make sure the shared volume is configured properly to prevent
        # possibility of split-brain/data corruption with ctdb service
        await self.middleware.call('ctdb.shared.volume.validate')

        if not info['started']:
            # start it if we get here
            await self.middleware.call('gluster.volume.start', {'name': CTDB_VOL_NAME})

        # try to mount it locally and send a request
        # to all the other peers in the TSP to also
        # FUSE mount it
        data = {'event': 'VOLUME_START', 'name': CTDB_VOL_NAME, 'forward': True}
        await self.middleware.call('gluster.localevents.send', data)

        # we need to wait on the local FUSE mount job since
        # ctdb daemon config is dependent on it being mounted
        fuse_mount_job = await self.middleware.call('core.get_jobs', [
            ('method', '=', 'gluster.fuse.mount'),
            ('arguments.0.name', '=', 'ctdb_shared_vol'),
            ('state', '=', 'RUNNING')
        ])
        if fuse_mount_job:
            wait_id = await self.middleware.call('core.job_wait', fuse_mount_job[0]['id'])
            await wait_id.wait()

        # Write our metadata configuration file
        await self.middleware.call('ctdb.shared.volume.config')

        # Initialize clustered secrets
        if not await self.middleware.call('clpwenc.check'):
            self.logger.debug('Generating clustered pwenc secret')
            await self.middleware.call('clpwenc.generate_secret')

        # The peers in the TSP could be using dns names while ctdb
        # only accepts IP addresses. This means we need to resolve
        # the hostnames of the peers in the TSP to their respective
        # IP addresses so we can write them to the ctdb private ip file.
        names = [i['hostname'] for i in await self.middleware.call('gluster.peer.query')]
        ips = await self.middleware.call('cluster.utils.resolve_hostnames', names)
        if len(names) != len(ips):
            # this means the gluster peers hostnames resolved to the same
            # ip address which is bad....in theory, this shouldn't occur
            # since adding gluster peers has it's own validation and would
            # cause it to fail way before this gets called but it's better
            # to be safe than sorry
            raise CallError('Duplicate gluster peer IP addresses detected.')

        # Setup the ctdb daemon config. Without ctdb daemon running, none of the
        # sharing services (smb/nfs) will work in an active-active setting.
        priv_ctdb_ips = [i['address'] for i in await self.middleware.call('ctdb.private.ips.query')]
        for ip_to_add in [i for i in ips if i not in [j for j in priv_ctdb_ips]]:
            ip_add_job = await self.middleware.call('ctdb.private.ips.create', {'ip': ip_to_add})
            try:
                await ip_add_job.wait(raise_error=True)
            except CallError as e:
                if e.errno == errno.EEXIST:
                    # This private IP has already been added. We can safely continue.
                    continue

                raise

        # this sends an event telling all peers in the TSP (including this system)
        # to start the ctdb service
        data = {'event': 'CTDB_START', 'name': CTDB_VOL_NAME, 'forward': True}
        await self.middleware.call('gluster.localevents.send', data)

        return await self.middleware.call('gluster.volume.query', [('name', '=', CTDB_VOL_NAME)])

    @job(lock=CRE_OR_DEL_LOCK)
    async def teardown(self, job, force=False):
        """
        If this method is called, it's expected that the end-user knows what they're doing. They
        also expect that this will _PERMANENTLY_ delete all the ctdb shared volume information. We
        also disable the glusterd service since that's what SMB service uses to determine if the
        system is in a "clustered" state. This method _MUST_ be called on each node in the cluster
        to fully "teardown" the cluster config.

        `force`: boolean, when True will forcefully stop all relevant cluster services before
            wiping the configuration.

        NOTE: THERE IS NO COMING BACK FROM THIS.
        """
        if not force:
            for vol in await self.middleware.call('gluster.volume.query'):
                if vol['name'] != CTDB_VOL_NAME:
                    # If someone calls this method, we expect that all other gluster volumes
                    # have been destroyed
                    raise CallError(f'{vol["name"]!r} must be removed before deleting {CTDB_VOL_NAME!r}')
        else:
            # we have to stop gluster service because it spawns a bunch of child processes
            # for the ctdb shared volume. This also stops ctdb, smb and unmounts all the
            # FUSE mountpoints.
            job.set_progress(50, 'Stopping cluster services')
            await self.middleware.call('service.stop', 'glusterd')

        job.set_progress(75, 'Removing cluster related configuration files and directories.')
        wipe_config_job = await self.middleware.call('cluster.utils.wipe_config')
        await wipe_config_job.wait()

        job.set_progress(99, 'Disabling cluster service')
        await self.middleware.call('service.update', 'glusterd', {'enable': False})
        await self.middleware.call('smb.reset_smb_ha_mode')

        job.set_progress(100, 'CTDB shared volume teardown complete.')

    @job(lock=CRE_OR_DEL_LOCK)
    async def delete(self, job):
        """
        Delete and unmount the shared volume used by ctdb daemon.
        """
        # nothing to delete if it doesn't exist
        info = await self.middleware.call('gluster.volume.exists_and_started', CTDB_VOL_NAME)
        if not info['exists']:
            return

        # stop the gluster volume
        if info['started']:
            options = {'args': (CTDB_VOL_NAME,), 'kwargs': {'force': True}}
            job.set_progress(33, f'Stopping gluster volume {CTDB_VOL_NAME!r}')
            await self.middleware.call('gluster.method.run', volume.stop, options)

        # finally, we delete it
        job.set_progress(66, f'Deleting gluster volume {CTDB_VOL_NAME!r}')
        await self.middleware.call('gluster.method.run', volume.delete, {'args': (CTDB_VOL_NAME,)})
        job.set_progress(100, f'Successfully deleted {CTDB_VOL_NAME!r}')
