import os
import time
import uuid
from contextlib import contextmanager
import pytest

from lakefs import client
from lakefs.repository import Repository

TEST_STORAGE_NAMESPACE_BASE = os.getenv("STORAGE_NAMESPACE", "").rstrip("/")


@contextmanager
def expect_exception_context(ex, status_code=None):
    try:
        yield
        assert 0, f"No exception raised! Expected exception of type {ex.__name__}"
    except ex as e:
        if status_code is not None:
            assert e.status_code == status_code


@pytest.fixture(name="test_name", autouse=True)
def fixture_test_name(request):
    return request.node.name.replace("_", "-")


@pytest.fixture(name="storage_namespace")
def fixture_storage_namespace(test_name):
    return f"{TEST_STORAGE_NAMESPACE_BASE}/{uuid.uuid1()}/{test_name}"


@pytest.fixture()
def setup_repo(storage_namespace, test_name):
    clt = client.DefaultClient
    repo_name = test_name + str(int(time.time()))
    default_branch = "main"
    repo = Repository(repo_name, clt)
    repo.create(storage_namespace, default_branch, True)
    return clt, repo
