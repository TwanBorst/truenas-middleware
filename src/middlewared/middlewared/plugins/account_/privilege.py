import enum
import errno

from middlewared.schema import accepts, Bool, Dict, Int, List, Ref, SID, Str, Patch
from middlewared.service import CallError, CRUDService, filter_list, private, ValidationErrors
from middlewared.service_exception import MatchNotFound
import middlewared.sqlalchemy as sa


class BuiltinPrivileges(enum.Enum):
    LOCAL_ADMINISTRATOR = "LOCAL_ADMINISTRATOR"


class PrivilegeModel(sa.Model):
    __tablename__ = "account_privilege"

    id = sa.Column(sa.Integer(), primary_key=True)
    builtin_name = sa.Column(sa.String(200), nullable=True)
    name = sa.Column(sa.String(200))
    local_groups = sa.Column(sa.JSON(type=list))
    ds_groups = sa.Column(sa.JSON(type=list))
    allowlist = sa.Column(sa.JSON(type=list))
    roles = sa.Column(sa.JSON(type=list))
    web_shell = sa.Column(sa.Boolean())


class PrivilegeService(CRUDService):

    keys = {}

    class Config:
        namespace = "privilege"
        datastore = "account.privilege"
        datastore_extend = "privilege.item_extend"
        datastore_extend_context = "privilege.item_extend_context"
        cli_namespace = "auth.privilege"

    ENTRY = Dict(
        "privilege_entry",
        Int("id"),
        Str("builtin_name", null=True),
        Str("name", required=True, empty=False),
        List("local_groups", items=[Int("local_group")]),
        List("ds_groups", items=[Int("ds_group_gid"), SID("ds_group_sid")]),
        List("allowlist", items=[Ref("allowlist_item")]),
        List("roles", items=[Str("role")]),
        Bool("web_shell", required=True),
    )

    @private
    async def item_extend_context(self, rows, extra):
        return {
            "groups": await self._groups(),
        }

    @private
    async def item_extend(self, item, context):
        item["local_groups"] = self._local_groups(context["groups"], item["local_groups"])
        item["ds_groups"] = await self._ds_groups(context["groups"], item["ds_groups"])
        return item

    @accepts(Patch(
        "privilege_entry",
        "privilege_create",
        ("rm", {"name": "builtin_name"}),
    ))
    async def do_create(self, data):
        """
        Creates a privilege.

        `name` is a name for privilege (must be unique).

        `local_groups` is a list of local user account group GIDs that gain this privilege.

        `ds_groups` is list of Directory Service group GIDs that will gain this privilege.

        `allowlist` is a list of API endpoints allowed for this privilege.

        `web_shell` controls whether users with this privilege are allowed to log in to the Web UI.
        """
        await self._validate("privilege_create", data)

        id = await self.middleware.call(
            "datastore.insert",
            self._config.datastore,
            data
        )

        return await self.get_instance(id)

    @accepts(
        Int("id", required=True),
        Patch(
            "privilege_entry",
            "privilege_update",
            ("rm", {"name": "builtin_name"}),
            ("attr", {"update": True}),
        )
    )
    async def do_update(self, id, data):
        """
        Update the privilege `id`.
        """
        old = await self.get_instance(id)
        new = old.copy()
        new["local_groups"] = [g["gid"] for g in new["local_groups"]]

        # Preference is for SID values rather than GIDS because they are universally unique
        new["ds_groups"] = []
        for g in old["ds_groups"]:
            new["ds_groups"].append(g["gid"] if not g["sid"] else g["sid"])

        new.update(data)

        verrors = ValidationErrors()

        if new["builtin_name"]:
            for k in ["name", "allowlist"]:
                if new[k] != old[k]:
                    verrors.add(f"privilege_update.{k}", "This field is read-only for built-in privileges")

            builtin_privilege = BuiltinPrivileges(new["builtin_name"])

            if builtin_privilege == BuiltinPrivileges.LOCAL_ADMINISTRATOR:
                if not await self.middleware.call("group.has_password_enabled_user", new["local_groups"]):
                    verrors.add(
                        "privilege_update.local_groups",
                        "None of the members of these groups has password login enabled. At least one grantee of "
                        "the \"Local Administrator\" privilege must have password login enabled."
                    )

        verrors.check()

        new.update(data)

        await self._validate("privilege_update", new, id)

        await self.middleware.call(
            "datastore.update",
            self._config.datastore,
            id,
            new,
        )

        return await self.get_instance(id)

    @accepts(
        Int("id")
    )
    async def do_delete(self, id):
        """
        Delete the privilege `id`.
        """
        privilege = await self.get_instance(id)
        if privilege["builtin_name"]:
            raise CallError("Unable to delete built-in privilege", errno.EPERM)

        response = await self.middleware.call(
            "datastore.delete",
            self._config.datastore,
            id
        )

        return response

    async def _validate(self, schema_name, data, id=None):
        verrors = ValidationErrors()

        await self._ensure_unique(verrors, schema_name, "name", data["name"], id)

        groups = await self._groups()
        for i, local_group_id in enumerate(data["local_groups"]):
            if not self._local_groups(groups, [local_group_id], include_nonexistent=False):
                verrors.add(f"{schema_name}.local_groups.{i}", "This local group does not exist")
        for i, ds_group_id in enumerate(data["ds_groups"]):
            if not await self._ds_groups(groups, [ds_group_id], include_nonexistent=False):
                verrors.add(f"{schema_name}.ds_groups.{i}", "This Directory Service group does not exist")

        for i, role in enumerate(data["roles"]):
            if role not in self.middleware.role_manager.roles:
                verrors.add(f"{schema_name}.roles.{i}", "Invalid role")

        if verrors:
            raise verrors

    async def _groups(self):
        groups = await self.middleware.call(
            "group.query",
            [],
            {"extra": {"additional_information": ["DS", "SMB"]}},
        )
        by_gid = {group["gid"]: group for group in groups}
        by_sid = {
            group["sid"]: group
            for group in filter_list(
                groups, [["sid", "!=", None], ["local", "=", False]],
            )
        }

        return {'by_gid': by_gid, 'by_sid': by_sid}

    def _local_groups(self, groups, local_groups, *, include_nonexistent=True):
        result = []
        for gid in local_groups:
            if group := groups['by_gid'].get(gid):
                if group["local"]:
                    result.append(group)
            else:
                if include_nonexistent:
                    result.append({
                        "gid": gid,
                        "group": None,
                    })

        return result

    async def _ds_groups(self, groups, ds_groups, *, include_nonexistent=True):
        """
        Directory services group privileges may assigned by either GID or SID.
        preference is for latter if it is available. The primary case where it
        will not be available is if this is not active directory.
        """
        result = []
        for xid in ds_groups:
            if isinstance(xid, int):
                if (group := groups['by_gid'].get(xid)) is None:
                    gid = xid
            else:
                if (group := groups['by_sid'].get(xid)) is None:
                    unixid = await self.middleware.call('idmap.sid_to_unixid', xid)
                    if unixid is None or unixid['id_type'] == 'USER':
                        gid = -1
                    else:
                        gid = unixid['id']

            if group is None:
                try:
                    group = await self.middleware.call(
                        "group.query",
                        [["gid", "=", gid]],
                        {
                            "extra": {"additional_information": ["DS", "SMB"]},
                            "get": True,
                        },
                    )
                except MatchNotFound:
                    if include_nonexistent:
                        result.append({
                            "gid": gid,
                            "sid": None,
                            "group": None,
                        })

                    continue

            if group["local"]:
                continue

            result.append(group)

        return result

    @private
    async def before_user_delete(self, user):
        for privilege in await self.middleware.call(
            'datastore.query',
            'account.privilege',
            [['builtin_name', '!=', None]],
        ):
            if not await self.middleware.call('group.has_password_enabled_user', privilege['local_groups'],
                                              [user['id']]):
                raise CallError(
                    f'After deleting this user no local user will have built-in privilege {privilege["name"]!r}.',
                    errno.EACCES,
                )

    @private
    async def before_group_delete(self, group):
        for privilege in await self.middleware.call('datastore.query', 'account.privilege'):
            if group['gid'] in privilege['local_groups']:
                raise CallError(
                    f'This group is used by privilege {privilege["name"]!r}. Please remove it from that privilege '
                    'first, then delete the group.',
                    errno.EACCES,
                )

    @private
    async def used_local_gids(self):
        gids = {}
        for privilege in await self.middleware.call('datastore.query', 'account.privilege', [], {'order_by': ['id']}):
            for gid in privilege['local_groups']:
                gids.setdefault(gid, privilege)

        return gids

    @private
    async def privileges_for_groups(self, groups_key, group_ids):
        """
        group_ids here are based on NSS group_list output.

        Directory services groups may have privileges assigned by SID, which
        are set on the domain controller rather than locally on TrueNAS.

        This means we expand the set of group_ids to include SID mappings for
        permissions evaluation.

        If for some reason libwbclient raises an exception during the attempt
        to convert unix gids to SIDs, then the domain is probably unhealthy and
        permissions failure is acceptable. We do not need to log here as there will
        be other failures / alerts and we don't want to spam logs unnecessarily.
        """
        if groups_key == 'ds_groups':
            try:
                sids = await self.middleware.call(
                    'idmap.convert_unixids',
                    [{'id_type': 'GROUP', 'id': x} for x in group_ids]
                )
            except Exception:
                group_ids = set(group_ids)
            else:
                group_ids = set(group_ids) | set([s['sid'] for s in sids['mapped'].values()])
        else:
            group_ids = set(group_ids)

        return [
            privilege for privilege in await self.middleware.call('datastore.query', 'account.privilege')
            if set(privilege[groups_key]) & group_ids
        ]

    @private
    async def compose_privilege(self, privileges):
        compose = {
            'roles': set(),
            'allowlist': [],
            'web_shell': False,
        }
        for privilege in privileges:
            for role in privilege['roles']:
                compose['roles'] |= self.middleware.role_manager.roles_for_fole(role)

                compose['allowlist'].extend(self.middleware.role_manager.allowlist_for_role(role))

            for item in privilege['allowlist']:
                compose['allowlist'].append(item)

            compose['web_shell'] |= privilege['web_shell']

        return compose

    @private
    async def full_privilege(self):
        return {
            'allowlist': [{'method': '*', 'resource': '*'}],
            'web_shell': True,
        }

    previous_always_has_root_password_enabled_value = None

    @private
    async def always_has_root_password_enabled(self, users=None, groups=None):
        if users is None:
            users = await self.middleware.call('user.query')
        if groups is None:
            groups = await self.middleware.call('group.query')

        root_user = filter_list(
            users,
            [['username', '=', 'root']],
            {'get': True},
        )
        users = await self.local_administrators([root_user['id']], groups)
        if not users:
            value = True
        else:
            value = False

            if self.previous_always_has_root_password_enabled_value:
                usernames = [user['username'] for user in users]
                self.middleware.send_event(
                    'user.web_ui_login_disabled', 'ADDED', id=None, fields={'usernames': usernames},
                )

        self.previous_always_has_root_password_enabled_value = value
        return value

    @private
    async def local_administrators(self, exclude_user_ids=None, groups=None):
        exclude_user_ids = exclude_user_ids or []
        if groups is None:
            groups = await self.middleware.call('group.query')

        local_administrator_privilege = await self.middleware.call(
            'datastore.query',
            'account.privilege',
            [['builtin_name', '=', BuiltinPrivileges.LOCAL_ADMINISTRATOR.value]],
            {'get': True},
        )
        return await self.middleware.call(
            'group.get_password_enabled_users',
            local_administrator_privilege['local_groups'],
            exclude_user_ids,
            groups,
        )


async def setup(middleware):
    middleware.event_register(
        'user.web_ui_login_disabled',
        'Sent when root user login to the Web UI is disabled.'
    )
