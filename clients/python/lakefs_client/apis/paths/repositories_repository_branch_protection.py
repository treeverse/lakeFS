from lakefs_client.paths.repositories_repository_branch_protection.get import ApiForget
from lakefs_client.paths.repositories_repository_branch_protection.post import ApiForpost
from lakefs_client.paths.repositories_repository_branch_protection.delete import ApiFordelete


class RepositoriesRepositoryBranchProtection(
    ApiForget,
    ApiForpost,
    ApiFordelete,
):
    pass
