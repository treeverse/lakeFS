from lakefs_client.paths.auth_policies.get import ApiForget
from lakefs_client.paths.auth_policies.post import ApiForpost


class AuthPolicies(
    ApiForget,
    ApiForpost,
):
    pass
