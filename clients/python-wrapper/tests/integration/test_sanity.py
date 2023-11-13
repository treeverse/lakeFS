import http

from lakefs import client
from lakefs.exceptions import ServerException, RepositoryNotFoundException
from lakefs.repository import Repository, RepositoryProperties
from tests.integration.conftest import expect_exception_context


def test_repository_sanity(storage_namespace):
    clt = client.DefaultClient
    repo_name = "my-repo"
    default_branch = "main"
    repo = Repository(repo_name, clt)
    repo_data = repo.create(storage_namespace, default_branch, True)
    expected_properties = RepositoryProperties(id=repo_name,
                                               default_branch=default_branch,
                                               storage_namespace=storage_namespace,
                                               creation_date=repo_data.properties.creation_date)
    assert repo_data.properties == expected_properties

    # Create with allow exists
    new_data = repo.create(storage_namespace, default_branch, True, exist_ok=True)
    assert new_data.properties == repo_data.properties

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
