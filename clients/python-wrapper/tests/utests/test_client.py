import os
import importlib

from tests.utests.common import (
    lakectl_test_config_context,
    lakectl_no_config_context,
    env_var_context, TEST_SERVER,
    TEST_ACCESS_KEY_ID,
    TEST_SECRET_ACCESS_KEY,
    TEST_ENDPOINT_PATH
)

from lakefs import config as client_config

TEST_CONFIG_KWARGS: dict[str, str] = {
    "username": "my_username",
    "password": "my_password",
    "host": "http://my_host",
    "access_token": "my_jwt_token"
}


class TestClient:
    def test_client_no_config(self, monkeypatch):
        with lakectl_no_config_context(monkeypatch) as client:
            client.DEFAULT_CLIENT = None
            client = importlib.reload(client)
            assert client.DEFAULT_CLIENT is None

    def test_client_no_kwargs(self, monkeypatch, tmp_path):
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            assert client.DEFAULT_CLIENT is not None
            config = client.DEFAULT_CLIENT.config
            assert config.host == TEST_SERVER + TEST_ENDPOINT_PATH
            assert config.username == TEST_ACCESS_KEY_ID
            assert config.password == TEST_SECRET_ACCESS_KEY

            with env_var_context():
                # Create a new client with env vars
                env_endpoint = "http://env_endpoint/api/v1"
                env_secret = "test_env_secret"
                os.environ[client_config._LAKECTL_ENDPOINT_ENV] = env_endpoint
                os.environ[client_config._LAKECTL_SECRET_ACCESS_KEY_ENV] = env_secret
                clt = client.Client()
                assert clt is not client.DEFAULT_CLIENT
                config = clt.config
                assert config.host == env_endpoint
                assert config.username == TEST_ACCESS_KEY_ID
                assert config.password == env_secret

    def test_client_kwargs(self, monkeypatch, tmp_path):
        # Use lakectl yaml file and ensure it is not being read in case kwargs are provided
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            clt = client.Client(**TEST_CONFIG_KWARGS)
            config = clt.config
            assert config.host == TEST_CONFIG_KWARGS["host"] + TEST_ENDPOINT_PATH
            assert config.username == TEST_CONFIG_KWARGS["username"]
            assert config.password == TEST_CONFIG_KWARGS["password"]
            assert config.access_token == TEST_CONFIG_KWARGS["access_token"]

    def test_client_init(self, monkeypatch, tmp_path):
        with lakectl_no_config_context(monkeypatch) as client:
            assert client.DEFAULT_CLIENT is None
            from lakefs.client import init
            init(**TEST_CONFIG_KWARGS)
            assert client.DEFAULT_CLIENT is not None
            config = client.DEFAULT_CLIENT.config
            assert config.host == TEST_CONFIG_KWARGS["host"] + TEST_ENDPOINT_PATH
            assert config.username == TEST_CONFIG_KWARGS["username"]
            assert config.password == TEST_CONFIG_KWARGS["password"]
            assert config.access_token == TEST_CONFIG_KWARGS["access_token"]
