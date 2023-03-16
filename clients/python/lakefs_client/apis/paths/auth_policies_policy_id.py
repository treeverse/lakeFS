from lakefs_client.paths.auth_policies_policy_id.get import ApiForget
from lakefs_client.paths.auth_policies_policy_id.put import ApiForput
from lakefs_client.paths.auth_policies_policy_id.delete import ApiFordelete


class AuthPoliciesPolicyId(
    ApiForget,
    ApiForput,
    ApiFordelete,
):
    pass
