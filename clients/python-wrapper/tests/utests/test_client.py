from lakefs.exceptions import NoAuthenticationFound
from tests.utests.common import (
    lakectl_test_config_context,
    lakectl_no_config_context,
    TEST_SERVER,
    TEST_ACCESS_KEY_ID,
    TEST_SECRET_ACCESS_KEY,
    TEST_ENDPOINT_PATH,
    TEST_CONFIG,
    expect_exception_context
)

TEST_CONFIG_KWARGS: dict[str, str] = {
    "username": "my_username",
    "password": "my_password",
    "host": "http://my_host",
    "access_token": "my_jwt_token"
}


class TestClient:
    def test_client_no_config(self, monkeypatch):
        with lakectl_no_config_context(monkeypatch) as client:
            with expect_exception_context(NoAuthenticationFound):
                client.Client()

    def test_client_no_kwargs(self, monkeypatch, tmp_path):
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            clt = client.Client()
            config = clt.config
            assert config.host == TEST_SERVER + TEST_ENDPOINT_PATH
            assert config.username == TEST_ACCESS_KEY_ID
            assert config.password == TEST_SECRET_ACCESS_KEY

    def test_client_kwargs(self, monkeypatch, tmp_path):
        # Use lakectl yaml file and ensure it is not being read in case kwargs are provided
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            clt = client.Client(**TEST_CONFIG_KWARGS)
            config = clt.config
            assert config.host == TEST_CONFIG_KWARGS["host"] + TEST_ENDPOINT_PATH
            assert config.username == TEST_CONFIG_KWARGS["username"]
            assert config.password == TEST_CONFIG_KWARGS["password"]
            assert config.access_token == TEST_CONFIG_KWARGS["access_token"]

    def test_client_with_config_file_env_var(self, monkeypatch, tmp_path):
        # Test that LAKECTL_CONFIG_FILE environment variable is respected
        cfg_file = tmp_path / "custom_config.yaml"
        cfg_file.write_bytes(TEST_CONFIG.encode())

        with lakectl_no_config_context(monkeypatch):
            monkeypatch.setenv("LAKECTL_CONFIG_FILE", str(cfg_file))
            # Import client module after setting env var
            from lakefs import client
            import importlib
            client = importlib.reload(client)
            try:
                clt = client.Client()
                config = clt.config
                assert config.host == TEST_SERVER + TEST_ENDPOINT_PATH
                assert config.username == TEST_ACCESS_KEY_ID
                assert config.password == TEST_SECRET_ACCESS_KEY
            finally:
                client._DEFAULT_CLIENT = None

    def test_client_config_file_env_var_precedence(self, monkeypatch, tmp_path):
        # Test that LAKECTL_CONFIG_FILE takes precedence over default path
        # Create a config in the default location with different values
        default_cfg = tmp_path / "default.yaml"
        default_cfg.write_bytes(b'''
server:
  endpoint_url: https://default_server

credentials:
    access_key_id: default_key
    secret_access_key: default_secret
''')

        # Create a config in custom location with the test values
        custom_cfg = tmp_path / "custom.yaml"
        custom_cfg.write_bytes(TEST_CONFIG.encode())

        with monkeypatch.context():
            from lakefs import config as client_config
            monkeypatch.setattr(client_config, "_LAKECTL_YAML_PATH", default_cfg)
            monkeypatch.setenv("LAKECTL_CONFIG_FILE", str(custom_cfg))

            from lakefs import client
            import importlib
            client = importlib.reload(client)
            try:
                clt = client.Client()
                config = clt.config
                # Should use custom config, not default
                assert config.host == TEST_SERVER + TEST_ENDPOINT_PATH
                assert config.username == TEST_ACCESS_KEY_ID
                assert config.password == TEST_SECRET_ACCESS_KEY
            finally:
                client._DEFAULT_CLIENT = None
