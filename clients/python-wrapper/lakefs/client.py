"""
lakeFS Client module
Handles authentication against the lakeFS server and wraps the underlying lakefs_sdk client

The client module holds a DefaultClient which will attempt to initialize on module loading using
environment credentials.
In case no credentials exist, a call to init() will be required or a Client object must be created explicitly

"""

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
    _server_conf: Optional[lakefs_sdk.Config] = None

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
        """
        Closes any open connections
        """
        if self.http_client is not None:
            self.http_client.close()

    @property
    def config(self):
        """
        Return the underlying lakefs_sdk configuration
        """
        return self._conf.configuration

    @property
    def sdk_client(self):
        """
        Return the underlying lakefs_sdk client
        """
        return self._client

    @property
    def storage_config(self):
        """
        lakeFS SDK storage config object, lazy evaluated.
        """
        if self._server_conf is None:
            self._server_conf = self._client.config_api.get_config()
        return lakefs_sdk.StorageConfig(**self._server_conf.storage_config.__dict__)

    @property
    def version_config(self) -> lakefs_sdk.VersionConfig:
        """
        lakeFS SDK version config object, lazy evaluated.
        """
        if self._server_conf is None:
            self._server_conf = self._client.config_api.get_config()
        return lakefs_sdk.VersionConfig(**self._server_conf.version_config.__dict__)


# global default client
DefaultClient: Optional[Client] = None

try:
    DefaultClient = Client()
except NoAuthenticationFound:
    # must call init() explicitly
    DefaultClient = None  # pylint: disable=C0103


def init(**kwargs) -> None:
    """
    Initialize DefaultClient using the provided parameters
    """
    global DefaultClient  # pylint: disable=W0603
    DefaultClient = Client(**kwargs)
