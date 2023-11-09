from lakefs.repository import Repository
from lakefs.branch import Branch

from tests.utests.common import get_test_client


def test_branch_creation():
    """
    Ensure branches can only be created in repo context
    """
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    repo.Branch("test_branch")
    try:
        Branch()
        assert 0
    except NotImplementedError:
        pass
