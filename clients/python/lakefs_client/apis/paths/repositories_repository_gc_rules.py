from lakefs_client.paths.repositories_repository_gc_rules.get import ApiForget
from lakefs_client.paths.repositories_repository_gc_rules.post import ApiForpost
from lakefs_client.paths.repositories_repository_gc_rules.delete import ApiFordelete


class RepositoriesRepositoryGcRules(
    ApiForget,
    ApiForpost,
    ApiFordelete,
):
    pass
