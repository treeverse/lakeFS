from lakefs.exceptions import NoAuthenticationFound
from tests.utests.common import (
    lakectl_test_config_context,
    lakectl_no_config_context,
    TEST_SERVER,
    TEST_ACCESS_KEY_ID,
    TEST_SECRET_ACCESS_KEY,
    TEST_ENDPOINT_PATH, expect_exception_context
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
