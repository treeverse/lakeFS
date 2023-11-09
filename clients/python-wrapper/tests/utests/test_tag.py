from lakefs.repository import Repository
from lakefs.tag import Tag
from tests.utests.common import get_test_client


def test_tag_creation():
    """
    Ensure tags can only be created in repo context
    """
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    repo.Tag("test_tag")
    try:
        Tag()
        assert 0
    except NotImplementedError:
        pass
