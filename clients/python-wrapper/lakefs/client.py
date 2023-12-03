"""
lakeFS Client module


Handles authentication against the lakeFS server and wraps the underlying lakefs_sdk client.
The client module holds a DefaultClient which will attempt to initialize on module loading using
environment credentials.
In case no credentials exist, a call to init() will be required or a Client object must be created explicitly

"""

from __future__ import annotations

from typing import Optional

import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

from lakefs.config import ClientConfig
from lakefs.exceptions import NoAuthenticationFound, NotAuthorizedException, ServerException
from lakefs.namedtuple import LenientNamedTuple

# global default client
DEFAULT_CLIENT: Optional[Client] = None


class ServerStorageConfiguration(LenientNamedTuple):
    """
    Represent a lakeFS server's storage configuration
    """
    blockstore_type: str
    pre_sign_support: bool
    import_support: bool
    blockstore_namespace_example: str
    blockstore_namespace_validity_regex: str
    pre_sign_support_ui: bool
    import_validity_regex: str
    default_namespace_prefix: Optional[str] = None


class ServerConfiguration:
    """
    Represent a lakeFS server's configuration
    """
    _conf: lakefs_sdk.Config
    _storage_conf: ServerStorageConfiguration

    def __init__(self, client: Optional[Client] = DEFAULT_CLIENT):
        try:
            self._conf = client.sdk_client.config_api.get_config()
            self._storage_conf = ServerStorageConfiguration(**self._conf.storage_config.dict())
        except lakefs_sdk.exceptions.ApiException as e:
            if isinstance(e, lakefs_sdk.exceptions.ApiException):
                raise NotAuthorizedException(e.status, e.reason) from e
            raise ServerException(e.status, e.reason) from e

    @property
    def version(self) -> str:
        """
        Return the lakeFS server version
        """
        return self._conf.version_config.version

    @property
    def storage_config(self) -> ServerStorageConfiguration:
        """
        Returns the lakeFS server storage configuration
        """
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
        self._client = LakeFSClient(self._conf.configuration, header_name='X-Lakefs-Client',
                                    header_value='python-lakefs')

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
    DEFAULT_CLIENT = Client()
except NoAuthenticationFound:
    # must call init() explicitly
    DEFAULT_CLIENT = None  # pylint: disable=C0103


def init(**kwargs) -> None:
    """
    Initialize DefaultClient using the provided parameters
    """
    global DEFAULT_CLIENT  # pylint: disable=W0603
    DEFAULT_CLIENT = Client(**kwargs)
