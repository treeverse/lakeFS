from lakefs.repository import Repository
from lakefs.reference import Reference

from tests.utests.common import get_test_client


def test_reference_creation():
    """
    Ensure references can only be created in repo context
    """
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    repo.Ref("test_reference")
    try:
        Reference()
        assert 0
    except NotImplementedError:
        pass
