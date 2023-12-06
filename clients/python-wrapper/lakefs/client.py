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
from lakefs.models import ServerStorageConfiguration

# global default client
_DEFAULT_CLIENT: Optional[Client] = None


def get_default_client() -> Client:
    """
    Returns client module's default client
    """
    return _DEFAULT_CLIENT


class ServerConfiguration:
    """
    Represent a lakeFS server's configuration
    """
    _conf: lakefs_sdk.Config
    _storage_conf: ServerStorageConfiguration

    def __init__(self, client: Optional[Client] = get_default_client()):
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

    Example of initializing a Client object:

    .. code-block:: python

        from lakefs import Client

        client = Client(username="<access_key_id>", password="<secret_access_key>", host="<lakefs_endpoint>")
        print(client.version)

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
    _DEFAULT_CLIENT = Client()
except NoAuthenticationFound:
    # must call init() explicitly
    _DEFAULT_CLIENT = None  # pylint: disable=C0103


def init(**kwargs) -> None:
    """
    Initialize DefaultClient using the provided parameters
    """
    global _DEFAULT_CLIENT  # pylint: disable=W0603
    _DEFAULT_CLIENT = Client(**kwargs)


class _BaseLakeFSObject:
    """
    Base class for all lakeFS SDK objects, holds the client object and handles errors where no authentication method
    found for client. Attempts to reload client dynamically in case of changes in the environment.
    """
    __client: Client

    def __init__(self, client: Optional[Client]):
        self.__client = client

    @property
    def _client(self):
        """
        If client is None due to missing authentication params, try to init again. If authentication method is still
        missing - will raise exception
        :return: The initialized client object
        :raise NoAuthenticationFound: If no authentication method found to configure the lakeFS client with
        """
        if self.__client is None:
            init()  # Try to init default client
            self.__client = _DEFAULT_CLIENT

        return self.__client
