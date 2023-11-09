import http
import time

from lakefs import client
from lakefs.exceptions import ServerException, RepositoryNotFoundException
from lakefs.repository import Repository
from tests.system_tests.conftest import expect_exception_context


def test_repository_sanity(storage_namespace):
    clt = client.DefaultClient
    repo_name = "my-repo"
    default_branch = "main"
    repo = Repository(repo_name, clt)
    repo_data = repo.create(storage_namespace, default_branch, True)
    now = time.time()
    assert repo_data.id == repo_name
    assert repo_data.storage_namespace == storage_namespace
    assert float(repo_data.creation_date) <= now
    assert repo_data.default_branch == default_branch

    # Create with allow exists
    new_data = repo.create(storage_namespace, default_branch, True, exist_ok=True)
    assert new_data == repo_data

    # Try to create twice and expect conflict
    with expect_exception_context(ServerException, http.HTTPStatus.CONFLICT.value):
        repo.create(storage_namespace, default_branch, True)

    # Get metadata
    _ = repo.metadata

    # Delete repository
    repo.delete()

    # Delete non existent
    with expect_exception_context(RepositoryNotFoundException):
        repo.delete()
