from lakefs.repository import Repository

from tests.utests.common import get_test_client


def test_reference_creation():
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    ref = repo.Ref("test_reference")
    assert ref.repo_id == "test_repo"
    assert ref.id == "test_reference"
