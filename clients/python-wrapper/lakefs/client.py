"""
lakeFS Client module
Handles authentication against the lakeFS server and wraps the underlying lakefs_sdk client

The client module holds a DefaultClient which will attempt to initialize on module loading using
environment credentials.
In case no credentials exist, a call to init() will be required or a Client object must be created explicitly

"""

from __future__ import annotations
from typing import Optional, NamedTuple

import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

from lakefs.config import ClientConfig
from lakefs.exceptions import NoAuthenticationFound

# global default client
DefaultClient: Optional[Client] = None


class ServerStorageConfiguration(NamedTuple):
    blockstore_type: str
    pre_sign_support: bool
    import_support: bool
    blockstore_namespace_example: str
    blockstore_namespace_validity_regex: str
    pre_sign_support_ui: bool
    import_validity_regex: str
    default_namespace_prefix: Optional[str] = None


class ServerConfiguration:
    _conf: lakefs_sdk.Config
    _storage_conf: ServerStorageConfiguration

    def __init__(self, client: Optional[Client] = DefaultClient):
        # TODO: try
        self._conf = client.sdk_client.config_api.get_config()
        self._storage_conf = ServerStorageConfiguration(**self._conf.storage_config.__dict__)

    @property
    def version(self) -> str:
        return self._conf.version_config.version

    @property
    def storage_config(self) -> ServerStorageConfiguration:
        return self._storage_conf


class Client:
    """
    Wrapper around lakefs_sdk's client object
    Takes care of instantiating it from the environment
    """

    _client: Optional[LakeFSClient] = None
    _conf: Optional[ClientConfig] = None
    _server_conf: Optional[ServerConfiguration] = None

    def __init__(self, **kwargs):
        self._conf = ClientConfig(**kwargs)
        self._client = LakeFSClient(self._conf.configuration)

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
            self._server_conf = ServerConfiguration(self)
        return self._server_conf.storage_config

    @property
    def version(self) -> str:
        """
        lakeFS Server version, lazy evaluated.
        """
        if self._server_conf is None:
            self._server_conf = ServerConfiguration(self)
        return self._server_conf.version


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
