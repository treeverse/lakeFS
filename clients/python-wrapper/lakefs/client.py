"""
lakeFS Client module

Handles authentication against the lakeFS server and wraps the underlying lakefs_sdk client.
"""

from __future__ import annotations

import datetime
from threading import Lock
from typing import Optional
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

from lakefs.config import ClientConfig
from lakefs.exceptions import NotAuthorizedException, ServerException, api_exception_handler
from lakefs.models import ServerStorageConfiguration
from lakefs.auth import access_token_from_aws_iam_role


if TYPE_CHECKING:
    import boto3

SINGLE_STORAGE_ID = ""

class ServerConfiguration:
    """
    Represent a lakeFS server's configuration
    """
    _conf: lakefs_sdk.Config
    _storage_conf: dict[str, ServerStorageConfiguration] = {}

    def __init__(self, client: Optional[Client] = None):
        try:
            self._conf = client.sdk_client.config_api.get_config()
            if self._conf.storage_config_list is not None:
                for storage in self._conf.storage_config_list:
                    self._storage_conf[storage.blockstore_id] = ServerStorageConfiguration(
                        **self._conf.storage_config.dict())
            if self._conf.storage_config is not None:
                self._storage_conf[SINGLE_STORAGE_ID] = ServerStorageConfiguration(**self._conf.storage_config.dict())

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
        Returns the default lakeFS server storage configuration
        """
        return self.storage_config_by_id()

    def storage_config_by_id(self, storage_id=SINGLE_STORAGE_ID):
        """
        Returns the lakeFS server storage configuration by ID
        """
        return self._storage_conf[storage_id]

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
        self._client = LakeFSClient(self._conf, header_name='X-Lakefs-Client',
                                    header_value='python-lakefs')
        self._server_conf = None
        self._reset_token_time = None
        self._session = None

        # Initialize auth if using IAM provider
        if self._conf.get_auth_type() is ClientConfig.AuthType.IAM:
            iam_provider = self._conf.iam_provider
            if iam_provider.type is ClientConfig.ProviderType.AWS_IAM:
                # boto3 session lazy loading (only if an AWS IAM provider is used)
                import boto3 # pylint: disable=import-outside-toplevel, import-error
                self._session = boto3.Session()
                lakefs_host = urlparse(self._conf.host).hostname
                self._conf.access_token, self._reset_token_time = access_token_from_aws_iam_role(
                    self._client,
                    lakefs_host,
                    self._session,
                    iam_provider.aws_iam
                )

    def __getattribute__(self, name):
        if name == "sdk_client":
            object.__getattribute__(self, "_refresh_token_if_necessary")()
        return object.__getattribute__(self, name)

    def _refresh_token_if_necessary(self):
        """
        Refresh the token if necessary
        """
        current_time = datetime.datetime.now(datetime.timezone.utc)
        if (self._conf.get_auth_type() is ClientConfig.AuthType.IAM and
                self._reset_token_time is not None and
                current_time >= self._reset_token_time):
            # Refresh token:
            iam_provider = self._conf.iam_provider
            if iam_provider.type == ClientConfig.ProviderType.AWS_IAM:
                lakefs_host = urlparse(self._conf.host).hostname
                self._conf.access_token, self._reset_token_time = access_token_from_aws_iam_role(
                    self._client,
                    lakefs_host,
                    self._session,
                    iam_provider.aws_iam
                )

    @property
    def config(self):
        """
        Return the underlying lakefs_sdk configuration
        """
        return self._conf

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
        return self.storage_config_by_id()

    @property
    def reset_time(self):
        """
        The time when the access token will expire.
        """
        return self._reset_token_time

    @reset_time.setter
    def reset_time(self, time: datetime):
        self._reset_token_time = time

    def storage_config_by_id(self, storage_id=SINGLE_STORAGE_ID):
        """
        Returns lakeFS SDK storage config object, defaults to a single storage ID.
        """
        if self._server_conf is None:
            self._server_conf = ServerConfiguration(self)
        return self._server_conf.storage_config_by_id(storage_id)

    @property
    def version(self) -> str:
        """
        lakeFS Server version, lazy evaluated.
        """
        if self._server_conf is None:
            self._server_conf = ServerConfiguration(self)
        return self._server_conf.version


def from_aws_role(
        session: boto3.Session,
        ttl_seconds: int = 3600,
        presigned_ttl: int = 60,
        additional_headers: dict[str, str] = None,
        **kwargs) -> Client:
    """
    Create a lakeFS client from an AWS role.
    :param session: : The boto3 session.
    :param ttl_seconds: The time-to-live for the generated lakeFS token in seconds. The default value is 3600 seconds.
    :param presigned_ttl: The time-to-live for the presigned URL in seconds. The default value is 60 seconds.
    :param additional_headers: Additional headers to include in the presigned URL.
    :param kwargs: The arguments to pass to the client.
    :return: A lakeFS client.
    """
    client = Client(**kwargs)
    lakefs_host = urlparse(client.config.host).hostname
    aws_provider_pros = ClientConfig.AWSIAMProviderConfig(
        token_ttl_seconds=ttl_seconds,
        url_presign_ttl_seconds=presigned_ttl,
        token_request_headers=additional_headers
    )
    access_token, reset_time = access_token_from_aws_iam_role(
        client.sdk_client,
        lakefs_host,
        session,
        aws_provider_pros
    )
    client.config.access_token = access_token
    client.reset_time = reset_time
    return client

def from_web_identity(code: str, state: str, redirect_uri: str, ttl_seconds: int = 3600, **kwargs) -> Client:
    """
    Authenticate against lakeFS using a code received from an identity provider

    :param code: The code received from the identity provider
    :param state: The state received from the identity provider
    :param redirect_uri: The redirect URI used in the authentication process
    :param ttl_seconds: The token's time-to-live in seconds
    :param kwargs: Remaining arguments for the Client object
    :return: The authenticated Client object
    :raise NotAuthorizedException: if user is not authorized to perform this operation
    """
    client = Client(**kwargs)
    sts_requests = lakefs_sdk.StsAuthRequest(code=code, state=state, redirect_uri=redirect_uri, ttl_seconds=ttl_seconds)
    with api_exception_handler():
        auth_token = client.sdk_client.experimental_api.sts_login(sts_requests)
    client.config.access_token = auth_token.token
    return client


class _BaseLakeFSObject:
    """
    Base class for all lakeFS SDK objects, holds the client object and handles errors where no authentication method
    found for client. Attempts to reload client dynamically in case of changes in the environment.
    """
    __mutex: Lock = Lock()
    __client: Optional[Client] = None

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
        if self.__client is not None:
            return self.__client

        with _BaseLakeFSObject.__mutex:
            if _BaseLakeFSObject.__client is None:
                _BaseLakeFSObject.__client = Client()
            return _BaseLakeFSObject.__client
