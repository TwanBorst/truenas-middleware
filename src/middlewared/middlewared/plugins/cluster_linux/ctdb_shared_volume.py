import errno
import json
import os
from pathlib import Path
from glustercli.cli import volume
from pyglfs import GLFSError
from time import sleep
from uuid import uuid4

from middlewared.service import Service, CallError, job
from middlewared.plugins.cluster_linux.utils import CTDBConfig
from middlewared.plugins.gluster_linux.utils import GlusterConfig, get_glusterd_uuid


MOUNT_UMOUNT_LOCK = CTDBConfig.MOUNT_UMOUNT_LOCK.value
CRE_OR_DEL_LOCK = CTDBConfig.CRE_OR_DEL_LOCK.value
LEGACY_CTDB_VOL_NAME = CTDBConfig.LEGACY_CTDB_VOL_NAME.value
CTDB_VOL_INFO_FILE = CTDBConfig.CTDB_VOL_INFO_FILE.value
METADATA_VOL_FILE = GlusterConfig.METADATA_VOLUME.value
CTDB_STATE_DIR = CTDBConfig.CTDB_STATE_DIR.value
MIGRATION_WAIT_TIME = 300


class CtdbSharedVolumeService(Service):

    class Config:
        namespace = 'ctdb.shared.volume'
        private = True

    def get_vol_and_path(self):
        try:
            with open(METADATA_VOL_FILE, 'r') as f:
                data = json.loads(f.read())

            return data
        except FileNotFoundError:
            pass

        vol_names = self.middleware.call_sync('gluster.volume.list')
        if LEGACY_CTDB_VOL_NAME in vol_names:
            try:
                self.middleware.call_sync('gluster.filesystem.lookup', {
                    'volume_name': LEGACY_CTDB_VOL_NAME,
                    'path': '.DEPRECATED'
                })
            except GLFSError as e:
                if e.errno != errno.ENOENT:
                    raise CallError(
                        'Failed to lookup DEPRECATED sentinel for legacy CTDB '
                        f'shared volume: {e.errmsg}', e.errno
                    )

                data = {
                    'volume': LEGACY_CTDB_VOL_NAME,
                    'system_dir': '/'
                }

                tmp_name = f'{METADATA_VOL_FILE}_{uuid4().hex}.tmp'
                with open(tmp_name, 'w') as f:
                    f.write(json.dumps(data))
                    f.flush()
                    os.fsync(f.fileno())

                os.rename(tmp_name, METADATA_VOL_FILE)
                return data

        for vol in vol_names:
            if vol == LEGACY_CTDB_VOL_NAME:
                continue

            try:
                self.middleware.call_sync('gluster.filesystem.lookup', {
                    'volume_name': vol,
                    'path': CTDB_STATE_DIR
                })
            except GLFSError as e:
                if e.errno != errno.ENOENT:
                    raise CallError(
                        'Failed to lookup truenas cluster state dir on '
                        f'volume [{vol}]: {e.errmsg}', e.errno
                    )

                continue

            data = {
                'volume': vol,
                'system_dir': CTDB_STATE_DIR
            }
            tmp_name = f'{METADATA_VOL_FILE}_{uuid4().hex}.tmp'
            with open(tmp_name, 'w') as f:
                f.write(json.dumps(data))
                f.flush()
                os.fsync(f.fileno())

            return data

        raise CallError("No metadata dir configured", errno.ENOENT)

    def generate_info(self):
        conf = self.get_vol_and_path()

        volume = conf['volume']
        volume_mp = Path(CTDBConfig.LOCAL_MOUNT_BASE.value, volume)
        system_dir = conf['system_dir']
        glfs_uuid = self.middleware.call_sync('gluster.filesystem.lookup', {
            'volume_name': volume,
            'path': system_dir
        })['uuid']
        data = {
            'volume_name': volume,
            'volume_mountpoint': str(volume_mp),
            'path': system_dir,
            'mountpoint': str(Path(f'{volume_mp}/{system_dir}')),
            'uuid': glfs_uuid
        }

        tmp_name = f'{CTDB_VOL_INFO_FILE}_{uuid4().hex}.tmp'
        with open(tmp_name, "w") as f:
            f.write(json.dumps(data))
            f.flush()
            os.fsync(f.fileno())

        os.rename(tmp_name, CTDB_VOL_INFO_FILE)
        return data

    def config(self):
        info_file = Path(CTDB_VOL_INFO_FILE)
        try:
            return json.loads(info_file.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return self.generate_info()

    @job(lock="ctdb_system_volume_update")
    def update(self, job, data):
        """
        update is performed under lock to serialize update ops
        and grant more visibility.
        """
        volume = data['name']
        uuid = data['uuid']
        info = self.middleware.call_sync('gluster.volume.exists_and_started', volume)
        if not info['exists']:
            raise CallError(
                f'{volume}: volume does not exist', errno.ENOENT
            )

        if not info['started']:
            self.middleware.call_sync('gluster.volume.start', {'name': volume})

        if self.middleware.call_sync('service.started', 'ctdb'):
            raise CallError(
                'Updates to TrueNAS clustered metadata volume are not '
                'permitted when the ctdb service is started'
            )

        tmp_name = f'{METADATA_VOL_FILE}_{uuid4().hex}.tmp'
        with open(tmp_name, 'w') as f:
            f.write(volume)
            f.flush()
            os.fsync(f.fileno())

        os.rename(tmp_name, METADATA_VOL_FILE)
        gluster_uuid = get_glusterd_uuid()

        try:
            self.middleware.call_sync('gluster.filesystem.unlink', {
                'volume_name': volume,
                'parent_uuid': uuid,
                'path': f'.{gluster_uuid}_UPDATE_IN_PROGRESS'
            })
        except Exception:
            self.logger.warning('Failed to unlink in-progress sentinel', exc_info=True)

        return self.generate_info()

    def __move_ctdb_vol(self, data):
        # do not call this method directly
        try:
            current = self.get_vol_and_path()
        except CallError as e:
            if e.errno != errno.ENOENT:
                raise

            self.logger.debug("Failed to detect metadata dir.", exc_info=True)
            current = {'volume': None}

        # If we're not moving the location, just make sure our vol file
        # is present
        if current['volume'] == data['name']:
            try:
                hdl = self.middleware.call_sync('gluster.filesystem.lookup', {
                    'volume_name': current['volume'],
                    'path': current['system_dir']
                })
            except GLFError as e:
                if e.errno != errno.ENOENT:
                    raise CallError(
                        'Failed to lookup truenas cluster state dir '
                        f'[{current["system_dir"]}] on volume '
                        f'[{current["volume"]}]: {e.errmsg}', e.errno
                    )

                hdl = self.middleware.call_sync('gluster.filesystem.mkdir', {
                    'volume_name': current['volume'],
                    'path': current['system_dir'],
                    'options': {'mode': 0o700}
                })
            return hdl['uuid']

        elif current['volume'] is None:
            hdl = self.middleware.call_sync('gluster.filesystem.mkdir', {
               'volume_name': data['name'],
               'path': CTDB_STATE_DIR,
               'options': {'mode': 0o700}
            })
            return hdl['uuid']

        src_uuid = self.middleware.call_sync('gluster.filesystem.lookup', {
            'volume_name': current['volume'],
            'path': current['system_dir']
        })['uuid']

        try:
            dst_uuid = self.middleware.call_sync('gluster.filesystem.lookup', {
                'volume_name': data['name'],
                'path': CTDB_STATE_DIR
            })['uuid']
        except GLFSError as e:
            if e.errno != errno.ENOENT:
                raise

            dst_uuid = self.middleware.call_sync('gluster.filesystem.mkdir', {
                'volume_name': data['name'],
                'path': CTDB_STATE_DIR,
                'options': {'mode': 0o700}
            })['uuid']

        move_job = self.middleware.call_sync('gluster.filesystem.copy_tree', {
            'src_volume_name': current['volume'],
            'src_uuid': src_uuid,
            'dst_volume_name': data['name'],
            'dst_uuid': dst_uuid,
        })
        move_job.wait_sync(raise_error=True)

        rmtree_job = self.middleware.call_sync('gluster.filesystem.rmtree', {
            'volume_name': current['volume'],
            'path': current['system_dir']
        })
        rmtree_job.wait_sync()

        if current['volume'] == LEGACY_CTDB_VOL_NAME:
            self.middleware.call_sync('gluster.filesystem.create_file', {
                'volume_name': current['volume'],
                'path': '.DEPRECATED',
                'options': {'mode': 0o700}
            })

        return dst_uuid

    @job(lock=CRE_OR_DEL_LOCK)
    def migrate(self, job, data):

        job.set_progress(0, f'Begining migration of cluster system config to {data["name"]} volume')
        if not self.middleware.call_sync('ctdb.general.healthy'):
            raise CallError(
                'CTDB volume configuration may only be initiated with healthy cluster'
            )

        cur_info = self.get_vol_and_path()
        job.set_progress(10, f'Begining migration of cluster system config from {cur_info["volume"]} to {data["name"]}')
        system_dir = self.middleware.call_sync('gluster.filesystem.lookup', {
            'volume_name': cur_info['volume'],
            'path': cur_info['system_dir']
        })
        contents = self.middleware.call_sync('gluster.filesystem.contents', {
            'volume_name': cur_info['volume'],
            'uuid': system_dir['uuid'],
        })

        for file in contents:
            if file.endswith('_UPDATE_IN_PROGRESS'):
                peer_uuid = file_name.strip('_UPDATE_IN_PROGRESS')
                raise CallError(
                    f'{peer_uuid[1:]}: Gluster peer currently in process '
                    'of updating system volume path', errno.EBUSY
                )

        job.set_progress(25, 'Stopping CTDB service on all nodes')
        # We must stop ctdbd fist (will fault cluster)
        self.middleware.call_sync('gluster.localevents.send', {
            'event': 'CTDB_STOP',
            'name': data['name'],
            'forward': True
        })

        # By the point ctdbd is stopped on all nodes
        # The following moves our nodes file and every other
        # ctdb / cluster-config related file to the new volume
        # then removes the original
        job.set_progress(50, 'Moving system files')
        dst_uuid = self.__move_ctdb_vol(data)

        job.set_progress(60, 'Setting update sentinels')
        for peer in self.middleware.call_sync('gluster.peer.query'):
            try:
                self.middleware.call_sync('gluster.filesystem.create_file', {
                    'volume_name': data['name'],
                    'uuid': dst_uuid,
                    'path': f'.{peer["uuid"]}_UPDATE_IN_PROGRESS'
                })
            except Exception:
                # We're already pretty heavily commited at this point
                # and so we'll just hope for the best that things shake out
                # okay.
                self.logger.warning(
                    'Failed to generate sentinel for node %s',
                    peer['hostname'], exc_info=True
                )

        # The event kicks off backgrounded change
        job.set_progress(75, 'Sending command to remote nodes to update config')
        self.middleware.call_sync('gluster.localevents.send', {
            'event': 'SYSTEM_VOL_CHANGE',
            'name': data['name'],
            'uuid': dst_uuid,
            'forward': True
        })

        # The cluster update is a backgrounded job, and may take a small
        # amount of time to complete.
        remaining = MIGRATION_WAIT_TIME
        while remaining > 0:
            contents = self.middleware.call_sync('gluster.filesystem.contents', {
                'volume_name': data['name'],
                'uuid': dst_uuid,
            })
            for file_name in contents:
                if not file_name.endswith('_UPDATE_IN_PROGRESS'):
                    continue

                peer_uuid = file_name.strip('_UPDATE_IN_PROGRESS')
                job.set_progress(80, f'Waiting for peer [{peer_uuid[1:]}] to finish update.')
                sleep(10)
                remaining -= 10

            break

        # This validates our nodes file and starts ctdb
        job.set_progress(90, 'Finalizing setup')
        self.middleware.call_sync('ctdb.shared.volume.create')

    async def validate(self, ctdb_vol_name):
        filters = [('id', '=', ctdb_vol_name)]
        ctdb = await self.middleware.call('gluster.volume.query', filters)
        if not ctdb:
            # it's expected that ctdb shared volume exists when
            # calling this method
            raise CallError(f'{ctdb_vol_name} does not exist', errno.ENOENT)

        # This should generate alert rather than exception
        """
        for i in ctdb:
            err_msg = f'A volume named "{ctdb_vol_name}" already exists '
            if i['type'] != 'REPLICATE':
                err_msg += (
                    'but is not a "REPLICATE" type volume. '
                    'Please delete or rename this volume and try again.'
                )
                raise CallError(err_msg)
            elif i['replica'] < 3 or i['num_bricks'] < 3:
                err_msg +=(
                    'but is configured in a way that '
                    'could cause data corruption. Please delete '
                    'or rename this volume and try again.'
                )
                raise CallError(err_msg)
        """
        return

    @job(lock=CRE_OR_DEL_LOCK)
    async def create(self, job):
        """
        Create and mount the shared volume to be used
        by ctdb daemon.
        """
        meta_config = await self.middleware.call('ctdb.shared.volume.get_vol_and_path')
        vol = meta_config['volume']
        info = await self.middleware.call('gluster.volume.exists_and_started', meta_config['volume'])

        # make sure the shared volume is configured properly to prevent
        # possibility of split-brain/data corruption with ctdb service
        await self.middleware.call('ctdb.shared.volume.validate', vol)

        if not info['started']:
            # start it if we get here
            await self.middleware.call('gluster.volume.start', {'name': meta_config['volume']})

        # try to mount it locally and send a request
        # to all the other peers in the TSP to also
        # FUSE mount it
        data = {'event': 'VOLUME_START', 'name': vol, 'forward': True}
        await self.middleware.call('gluster.localevents.send', data)

        # we need to wait on the local FUSE mount job since
        # ctdb daemon config is dependent on it being mounted
        fuse_mount_job = await self.middleware.call('core.get_jobs', [
            ('method', '=', 'gluster.fuse.mount'),
            ('arguments.0.name', '=', vol),
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
        data = {'event': 'CTDB_START', 'name': meta_config['volume'], 'forward': True}
        await self.middleware.call('gluster.localevents.send', data)

        return await self.middleware.call('gluster.volume.query', [('name', '=', vol)])

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
