import os
import re
import time
import uuid
import pytest

from lakefs import client
from lakefs.repository import Repository

TEST_STORAGE_NAMESPACE_BASE = os.getenv("STORAGE_NAMESPACE", "").rstrip("/")


def get_storage_namespace(test_name):
    return f"{TEST_STORAGE_NAMESPACE_BASE}/{uuid.uuid1()}/{test_name}"


def _setup_repo(namespace, name, default_branch):
    clt = client.DEFAULT_CLIENT
    repo_name = name + str(int(time.time()))
    repo = Repository(repo_name, clt)
    repo.create(storage_namespace=namespace, default_branch=default_branch)
    return clt, repo


@pytest.fixture(name="test_name")
def fixture_test_name(request):
    return re.sub(r'[_\[\]]', "-", request.node.name.lower())


@pytest.fixture(name="storage_namespace")
def fixture_storage_namespace(test_name):
    return get_storage_namespace(test_name)


@pytest.fixture()
def setup_repo(storage_namespace, test_name, default_branch="main"):
    return _setup_repo(storage_namespace, test_name, default_branch)


@pytest.fixture(scope="session")
def setup_branch_with_commits():
    _, repo = _setup_repo(get_storage_namespace("branch-with-commits"),
                          "branch-with-commits",
                          "main")
    branch = repo.branch("test_branch").create("main")
    commit_num = 199
    for i in range(commit_num):
        obj = branch.object("test1")
        if not i % 2:
            obj.upload("test_data")
        else:
            obj.delete()
        branch.commit(f"commit {commit_num - i - 1}")
    return branch


@pytest.fixture(name="pre_sign", scope="function")
def fixture_pre_sign(request):
    clt = client.DEFAULT_CLIENT
    if request.param and not clt.storage_config.pre_sign_support:
        pytest.skip("Storage adapter does not support pre-sign mode")
    return request.param
