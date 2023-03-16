from lakefs_client.paths.repositories_repository.get import ApiForget
from lakefs_client.paths.repositories_repository.delete import ApiFordelete


class RepositoriesRepository(
    ApiForget,
    ApiFordelete,
):
    pass
