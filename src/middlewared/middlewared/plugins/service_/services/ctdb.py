from .base import SimpleService


class CtdbService(SimpleService):

    name = 'ctdb'
    systemd_unit = 'ctdb'
    etc = ['ctdb']

    async def before_start(self):
        self.middleware.logger.debug("XXX: entered before start")
        # we need to make sure the private/public
        # ip files for ctdb are symlinked to the
        # ctdb shared volume (if appropriate)
        if (await self.middleware.call('ctdb.setup.init'))['logit']:
            self.middleware.logger.error('ctdb config setup failed, check logs')
        self.middleware.logger.debug("XXX: finished setup")

    async def after_start(self):
        self.middleware.logger.debug("XXX: entered after start")
        await self.middleware.call('ctdb.event.scripts.init')
        await self.middleware.call('ctdb.setup.public_ip_file')
        await self.middleware.call('smb.reset_smb_ha_mode')
        await self.middleware.call('etc.generate', 'smb')
        self.middleware.logger.debug("XXX: finished after start")

    async def after_stop(self):
        if not await self.middleware.call('cluster.utils.is_clustered'):
            await self.middleware.call('smb.reset_smb_ha_mode')
            await self.middleware.call('etc.generate', 'smb')

        await self.middleware.call('tdb.close_cluster_handles')
