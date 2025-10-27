from tests.utests.common import get_test_client

from lakefs.repository import Repository


def test_tag_creation():
    """
    Ensure tags can only be created in repo context
    """
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    tag = repo.tag("test_tag")
    assert tag.repo_id == "test_repo"
    assert tag.id == "test_tag"
