import http
import time

import lakefs_sdk

from tests.utests.common import get_test_repo, TEST_REPO_ARGS, expect_exception_context

from lakefs.exceptions import ServerException, NotAuthorizedException, NotFoundException, ConflictException
from lakefs.repository import RepositoryProperties


def test_repository_creation(monkeypatch):
    repo = get_test_repo()
    with monkeypatch.context():
        def monkey_create_repository(_self, repository_creation, *_):
            assert repository_creation.name == TEST_REPO_ARGS.name
            assert repository_creation.storage_namespace == TEST_REPO_ARGS.storage_namespace
            assert repository_creation.default_branch == TEST_REPO_ARGS.default_branch
            assert repository_creation.sample_data == TEST_REPO_ARGS.sample_data
            return lakefs_sdk.Repository(id=TEST_REPO_ARGS.name,
                                         creation_date=int(time.time()),
                                         storage_namespace=TEST_REPO_ARGS.storage_namespace,
                                         default_branch=TEST_REPO_ARGS.default_branch)

        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "create_repository", monkey_create_repository)
        repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace,
                    default_branch=TEST_REPO_ARGS.default_branch,
                    include_samples=TEST_REPO_ARGS.sample_data)


def test_repository_creation_already_exists(monkeypatch):
    repo = get_test_repo()
    ex = lakefs_sdk.exceptions.ApiException(status=http.HTTPStatus.CONFLICT.value)

    with monkeypatch.context():
        def monkey_create_repository(*_):
            raise ex

        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "create_repository", monkey_create_repository)

        # Expect success when exist_ok = True
        existing = lakefs_sdk.Repository(id=TEST_REPO_ARGS.name,
                                         default_branch="main",
                                         storage_namespace="s3://existing-namespace",
                                         creation_date=12345)

        def monkey_get_repository(*_):
            return existing

        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "get_repository", monkey_get_repository)
        res = repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace,
                          default_branch=TEST_REPO_ARGS.default_branch,
                          include_samples=TEST_REPO_ARGS.sample_data,
                          exist_ok=True)

        assert res.properties == RepositoryProperties(**existing.dict())

    # Expect fail on exists
    with expect_exception_context(ConflictException):
        repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace,
                    default_branch=TEST_REPO_ARGS.default_branch,
                    include_samples=TEST_REPO_ARGS.sample_data)

    # Expect fail on exists
    ex = lakefs_sdk.exceptions.UnauthorizedException(http.HTTPStatus.UNAUTHORIZED)
    with expect_exception_context(NotAuthorizedException):
        repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace,
                    default_branch=TEST_REPO_ARGS.default_branch,
                    include_samples=TEST_REPO_ARGS.sample_data)


def test_delete_repository(monkeypatch):
    repo = get_test_repo()
    with monkeypatch.context():
        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "delete_repository", lambda *args: None)
        repo.delete()

        ex = None

        def monkey_delete_repository(*_):
            raise ex

        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "delete_repository", monkey_delete_repository)
        # Not found
        ex = lakefs_sdk.exceptions.NotFoundException(status=http.HTTPStatus.NOT_FOUND)
        with expect_exception_context(NotFoundException):
            repo.delete()

        # Unauthorized
        ex = lakefs_sdk.exceptions.UnauthorizedException(status=http.HTTPStatus.UNAUTHORIZED)
        with expect_exception_context(NotAuthorizedException):
            repo.delete()

        # Other error
        ex = lakefs_sdk.exceptions.ApiException()
        with expect_exception_context(ServerException):
            repo.delete()
