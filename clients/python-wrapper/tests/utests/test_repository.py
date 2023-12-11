import http
import os
import time

import lakefs_sdk

from tests.utests.common import (
    get_test_repo,
    expect_exception_context,
    lakectl_no_config_context,
    env_var_context,
    TEST_REPO_ARGS
)

from lakefs.exceptions import (
    ServerException,
    NotAuthorizedException,
    NotFoundException,
    ConflictException,
    NoAuthenticationFound
)
from lakefs import RepositoryProperties


def monkey_create_repository(_self, repository_creation, *_):
    assert repository_creation.name == TEST_REPO_ARGS.name
    assert repository_creation.storage_namespace == TEST_REPO_ARGS.storage_namespace
    assert repository_creation.default_branch == TEST_REPO_ARGS.default_branch
    assert repository_creation.sample_data == TEST_REPO_ARGS.sample_data
    return lakefs_sdk.Repository(id=TEST_REPO_ARGS.name,
                                 creation_date=int(time.time()),
                                 storage_namespace=TEST_REPO_ARGS.storage_namespace,
                                 default_branch=TEST_REPO_ARGS.default_branch)


def test_repository_creation(monkeypatch):
    repo = get_test_repo()

    with monkeypatch.context():
        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "create_repository", monkey_create_repository)
        repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace,
                    default_branch=TEST_REPO_ARGS.default_branch,
                    include_samples=TEST_REPO_ARGS.sample_data)


def test_repository_creation_already_exists(monkeypatch):
    repo = get_test_repo()
    ex = lakefs_sdk.exceptions.ApiException(status=http.HTTPStatus.CONFLICT.value)

    with monkeypatch.context():
        def monkey_create_repository_ex(*_):
            raise ex

        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "create_repository", monkey_create_repository_ex)

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


def test_create_repository_no_authentication(monkeypatch):
    with lakectl_no_config_context(monkeypatch):
        from lakefs.repository import Repository
        repo = Repository("test-repo", None)
        with expect_exception_context(NoAuthenticationFound):
            repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace)

        # update credentials and retry create
        monkeypatch.setattr(lakefs_sdk.RepositoriesApi, "create_repository", monkey_create_repository)
        with env_var_context():
            from lakefs import config as client_config
            # Create a new client with env vars
            os.environ[client_config._LAKECTL_ENDPOINT_ENV] = "endpoint"
            os.environ[client_config._LAKECTL_SECRET_ACCESS_KEY_ENV] = "secret"
            os.environ[client_config._LAKECTL_ACCESS_KEY_ID_ENV] = "key"
            repo.create(storage_namespace=TEST_REPO_ARGS.storage_namespace,
                        default_branch=TEST_REPO_ARGS.default_branch)
