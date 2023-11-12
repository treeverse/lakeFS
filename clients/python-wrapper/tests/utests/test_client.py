import os
import importlib
from copy import deepcopy
from contextlib import contextmanager

from lakefs import config as client_config

TEST_SERVER = "https://test_server/api/v1"
TEST_ACCESS_KEY_ID = "test_access_key_id"
TEST_SECRET_ACCESS_KEY = "test_secret_access_key"
TEST_CONFIG = f'''
server:
  endpoint_url: {TEST_SERVER}

credentials:
    access_key_id: {TEST_ACCESS_KEY_ID}
    secret_access_key: {TEST_SECRET_ACCESS_KEY}
'''

TEST_CONFIG_KWARGS = {
    "username": "my_username",
    "password": "my_password",
    "host": "http://my_host/api/v1",
    "access_token": "my_jwt_token"
}


@contextmanager
def env_var_context():
    old_env = deepcopy(os.environ)
    try:
        yield
    finally:
        os.environ = old_env


@contextmanager
def lakectl_no_config_context(monkey):
    with monkey.context():
        monkey.setattr(client_config, "_LAKECTL_YAML_PATH", "file_not_found")
        from lakefs import client  # Must be imported after the monkey patching
        yield client


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


class TestClient:
    def test_client_no_config(self, monkeypatch):
        with lakectl_no_config_context(monkeypatch) as client:
            client.DefaultClient = None
            client = importlib.reload(client)
            assert client.DefaultClient is None

    def test_client_no_kwargs(self, monkeypatch, tmp_path):
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            assert client.DefaultClient is not None
            config = client.DefaultClient.config
            assert config.host == TEST_SERVER
            assert config.username == TEST_ACCESS_KEY_ID
            assert config.password == TEST_SECRET_ACCESS_KEY

            with env_var_context():
                # Create a new client with env vars
                env_endpoint = "http://env_endpoint/api/v1"
                env_secret = "test_env_secret"
                os.environ[client_config._LAKECTL_ENDPOINT_ENV] = env_endpoint
                os.environ[client_config._LAKECTL_SECRET_ACCESS_KEY_ENV] = env_secret
                clt = client.Client()
                assert clt is not client.DefaultClient
                config = clt.config
                assert config.host == env_endpoint
                assert config.username == TEST_ACCESS_KEY_ID
                assert config.password == env_secret

    def test_client_kwargs(self, monkeypatch, tmp_path):
        # Use lakectl yaml file and ensure it is not being read in case kwargs are provided
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            clt = client.Client(**TEST_CONFIG_KWARGS)
            config = clt.config
            assert config.host == TEST_CONFIG_KWARGS["host"]
            assert config.username == TEST_CONFIG_KWARGS["username"]
            assert config.password == TEST_CONFIG_KWARGS["password"]
            assert config.access_token == TEST_CONFIG_KWARGS["access_token"]

    def test_client_init(self, monkeypatch, tmp_path):
        with lakectl_no_config_context(monkeypatch) as client:
            assert client.DefaultClient is None
            from lakefs.client import init
            init(**TEST_CONFIG_KWARGS)
            assert client.DefaultClient is not None
            config = client.DefaultClient.config
            assert config.host == TEST_CONFIG_KWARGS["host"]
            assert config.username == TEST_CONFIG_KWARGS["username"]
            assert config.password == TEST_CONFIG_KWARGS["password"]
            assert config.access_token == TEST_CONFIG_KWARGS["access_token"]
