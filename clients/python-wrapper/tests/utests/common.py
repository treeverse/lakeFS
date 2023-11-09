import importlib
import os
from contextlib import contextmanager
from copy import deepcopy

import lakefs_sdk
import lakefs.repository
from lakefs import config as client_config

TEST_SERVER = "https://test_server"
TEST_ACCESS_KEY_ID = "test_access_key_id"
TEST_SECRET_ACCESS_KEY = "test_secret_access_key"
TEST_CONFIG = f'''
server:
  endpoint_url: {TEST_SERVER}

credentials:
    access_key_id: {TEST_ACCESS_KEY_ID}
    secret_access_key: {TEST_SECRET_ACCESS_KEY}
'''

TEST_CONFIG_KWARGS: dict[str, str] = {
    "username": "my_username",
    "password": "my_password",
    "host": "http://my_host",
    "access_token": "my_jwt_token"
}

TEST_ENDPOINT_PATH = "/api/v1"

TEST_REPO_ARGS = lakefs_sdk.RepositoryCreation(name="test-repo",
                                               storage_namespace="s3://test_namespace",
                                               default_branch="default-branch",
                                               samples_data=True)


def get_test_client():
    from lakefs.client import Client
    clt = Client(username=TEST_ACCESS_KEY_ID, password=TEST_SECRET_ACCESS_KEY, host=TEST_SERVER)
    return clt


@contextmanager
def lakectl_test_config_context(monkey, tmp_path):
    cfg_file = tmp_path / "test.yaml"
    cfg_file.write_bytes(TEST_CONFIG.encode())
    with monkey.context():
        monkey.setattr(client_config, "_LAKECTL_YAML_PATH", cfg_file)
        from lakefs import client  # Must be imported after the monkey patching
        client = importlib.reload(client)
        try:
            yield client
        finally:
            client.DefaultClient = None


@contextmanager
def lakectl_no_config_context(monkey):
    with monkey.context():
        monkey.setattr(client_config, "_LAKECTL_YAML_PATH", "file_not_found")
        from lakefs import client  # Must be imported after the monkey patching
        yield client


@contextmanager
def env_var_context():
    old_env = deepcopy(os.environ)
    try:
        yield
    finally:
        os.environ = old_env


def get_test_repo() -> lakefs.repository.Repository:
    from lakefs.client import Client
    client = Client(username="test_user", password="test_password", host="http://127.0.0.1:8000")
    return lakefs.repository.Repository(repository_id=TEST_REPO_ARGS.name, client=client)
