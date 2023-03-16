from lakefs_client.paths.auth_users.get import ApiForget
from lakefs_client.paths.auth_users.post import ApiForpost


class AuthUsers(
    ApiForget,
    ApiForpost,
):
    pass
