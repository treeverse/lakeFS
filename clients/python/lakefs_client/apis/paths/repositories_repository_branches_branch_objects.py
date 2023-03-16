from lakefs_client.paths.repositories_repository_branches_branch_objects.put import ApiForput
from lakefs_client.paths.repositories_repository_branches_branch_objects.post import ApiForpost
from lakefs_client.paths.repositories_repository_branches_branch_objects.delete import ApiFordelete


class RepositoriesRepositoryBranchesBranchObjects(
    ApiForput,
    ApiForpost,
    ApiFordelete,
):
    pass
