import os
import importlib
from copy import deepcopy
from contextlib import contextmanager
from pylotl import config as client_config

test_server = "https://test_server/api/v1"
test_access_key_id = "test_access_key_id"
test_secret_access_key = "test_secret_access_key"
test_config = f'''
server:
  endpoint_url: {test_server}

credentials:
    access_key_id: {test_access_key_id}
    secret_access_key: {test_secret_access_key}
'''

test_config_kwargs = {
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
        from pylotl import client  # Must be imported after the monkey patching
        yield client


@contextmanager
def lakectl_test_config_context(monkey, tmp_path):
    cfg_file = tmp_path / "test.yaml"
    cfg_file.write_bytes(test_config.encode())
    with monkey.context():
        monkey.setattr(client_config, "_LAKECTL_YAML_PATH", cfg_file)
        from pylotl import client  # Must be imported after the monkey patching
        client = importlib.reload(client)
        try:
            yield client
        finally:
            client.DefaultClient = None


class TestClient:
    def test_client_no_config(self, monkeypatch):
        with lakectl_no_config_context(monkeypatch) as client:
            assert client.DefaultClient is None

    def test_client_no_kwargs(self, monkeypatch, tmp_path):
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            assert client.DefaultClient is not None
            config = client.DefaultClient.config
            assert config.host == test_server
            assert config.username == test_access_key_id
            assert config.password == test_secret_access_key

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
                assert config.username == test_access_key_id
                assert config.password == env_secret

    def test_client_kwargs(self, monkeypatch, tmp_path):
        # Use lakectl yaml file and ensure it is not being read in case kwargs are provided
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            clt = client.Client(**test_config_kwargs)
            config = clt.config
            assert config.host == test_config_kwargs["host"]
            assert config.username == test_config_kwargs["username"]
            assert config.password == test_config_kwargs["password"]
            assert config.access_token == test_config_kwargs["access_token"]

    def test_client_init(self, monkeypatch, tmp_path):
        with lakectl_no_config_context(monkeypatch) as client:
            assert client.DefaultClient is None
            from pylotl.client import init
            init(**test_config_kwargs)
            assert client.DefaultClient is not None
            config = client.DefaultClient.config
            assert config.host == test_config_kwargs["host"]
            assert config.username == test_config_kwargs["username"]
            assert config.password == test_config_kwargs["password"]
            assert config.access_token == test_config_kwargs["access_token"]
