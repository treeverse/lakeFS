import os
from lakefs import client


def test_sanity_client():
    clt = client.DefaultClient
    assert clt is not None
    key = os.getenv("LAKECTL_CREDENTIALS_ACCESS_KEY_ID")
    secret = os.getenv("LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY")
    host = os.getenv("LAKECTL_SERVER_ENDPOINT_URL")
    assert clt.config.username == key
    assert clt.config.password == secret
    assert clt.config.host == host

    version = os.getenv("TAG")
    assert clt.version_config.version == version
