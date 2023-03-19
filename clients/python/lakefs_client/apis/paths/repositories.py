from lakefs_client.paths.repositories.get import ApiForget
from lakefs_client.paths.repositories.post import ApiForpost


class Repositories(
    ApiForget,
    ApiForpost,
):
    pass
