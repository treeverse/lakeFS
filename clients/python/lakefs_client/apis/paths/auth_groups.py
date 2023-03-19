from lakefs_client.paths.auth_groups.get import ApiForget
from lakefs_client.paths.auth_groups.post import ApiForpost


class AuthGroups(
    ApiForget,
    ApiForpost,
):
    pass
