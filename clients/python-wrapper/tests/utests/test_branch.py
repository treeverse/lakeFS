from lakefs.repository import Repository

from tests.utests.common import get_test_client


def test_branch_creation():
    """
    Ensure branches can only be created in repo context
    """
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    branch = repo.branch("test_branch")
    assert branch.repo_id == "test_repo"
    assert branch.id == "test_branch"
