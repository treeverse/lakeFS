from lakefs_client.paths.setup_lakefs.get import ApiForget
from lakefs_client.paths.setup_lakefs.post import ApiForpost


class SetupLakefs(
    ApiForget,
    ApiForpost,
):
    pass
