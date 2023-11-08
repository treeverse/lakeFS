from typing import Optional
import requests
from requests.auth import HTTPBasicAuth

import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

from lakefs.config import ClientConfig
from lakefs.exceptions import NoAuthenticationFound


class Client:
    """
    Wrapper around lakefs_sdk's client object
    Takes care of instantiating it from the environment
    """

    _client: Optional[LakeFSClient] = None
    http_client: Optional[requests.Session] = None
    _conf: Optional[ClientConfig] = None
    _storage_conf: Optional[lakefs_sdk.StorageConfig] = None

    def __init__(self, **kwargs):
        self._conf = ClientConfig(**kwargs)
        self._client = LakeFSClient(self._conf.configuration)

        # Set up http client
        config = self._conf.configuration
        headers = {}
        auth = None
        if config.access_token is not None:
            # TODO: Create custom auth class and inherit from BaseAuth
            headers["Authorization"] = f"Bearer {config.access_token}"

        if config.username is not None and config.password is not None:
            auth = HTTPBasicAuth(config.username, config.password)

        self.http_client = requests.Session()
        self.http_client.headers = headers
        self.http_client.auth = auth

    def close(self):
        if self.http_client is not None:
            self.http_client.close()

    @property
    def config(self):
        return self._conf.configuration

    @property
    def sdk_client(self):
        return self._client

    @property
    def storage_config(self):
        if self._storage_conf is None:
            self._storage_conf = self._client.internal_api.get_storage_config()
        return lakefs_sdk.StorageConfig(**self._storage_conf.__dict__)


# global default client
DefaultClient: Optional[Client] = None

try:
    DefaultClient = Client()
except NoAuthenticationFound:
    # must call init() explicitly
    DefaultClient = None


def init(**kwargs):
    global DefaultClient
    DefaultClient = Client(**kwargs)
