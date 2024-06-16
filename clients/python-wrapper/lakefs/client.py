"""
lakeFS Client module

Handles authentication against the lakeFS server and wraps the underlying lakefs_sdk client.
"""

from __future__ import annotations

import base64
import json
from threading import Lock
from typing import Optional
from urllib.parse import urlparse, parse_qs

import lakefs_sdk
from lakefs_sdk import ExternalLoginInformation
from lakefs_sdk.client import LakeFSClient

from lakefs.config import ClientConfig
from lakefs.exceptions import NotAuthorizedException, ServerException, api_exception_handler
from lakefs.models import ServerStorageConfiguration


class ServerConfiguration:
    """
    Represent a lakeFS server's configuration
    """
    _conf: lakefs_sdk.Config
    _storage_conf: ServerStorageConfiguration

    def __init__(self, client: Optional[Client] = None):
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
        self._client = LakeFSClient(self._conf, header_name='X-Lakefs-Client',
                                    header_value='python-lakefs')

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


def get_identity_token(session: 'boto3.Session') -> str:
    """
   Generate the identity token required for lakeFS authentication from an AWS session.

   This function uses the STS client to generate a presigned URL for the `get_caller_identity` action, extracts the required values from the URL,
   and creates a base64-encoded JSON object with these values.

   :param session: A boto3 session object with the necessary AWS credentials and region information.
   :return: A base64-encoded JSON string containing the required authentication information.
   :raises ValueError: If the session does not have a region name set.
   """

    if session.region_name is None:
        raise ValueError("Region name is not set in the session")

    from botocore.client import Config
    sts_client = session.client('sts', config=Config(signature_version='v4'))
    presigned_url = sts_client.generate_presigned_url('get_caller_identity', Params={}, ExpiresIn=3600)

    # Parse the URL to extract components
    parsed_url = urlparse(presigned_url)
    query_params = parse_qs(parsed_url.query)

    # Extract values from query parameters
    json_object = {
        "method": "POST",
        "host": parsed_url.hostname,
        "region": session.region_name,
        "action": query_params['Action'][0],
        "date": query_params['X-Amz-Date'][0],
        "expiration_duration": "3600",  # 1 hour in seconds
        "access_key_id": query_params['X-Amz-Credential'][0].split('/')[0],
        "signature": query_params['X-Amz-Signature'][0],
        "signed_headers": query_params['X-Amz-SignedHeaders'],
        "version": query_params['Version'][0],
        "algorithm": query_params['X-Amz-Algorithm'][0],
        "security_token": query_params.get('X-Amz-Security-Token', [None])[0]
    }
    json_string = json.dumps(json_object)
    return base64.b64encode(json_string.encode('utf-8')).decode('utf-8')


def from_aws_role(session: 'boto3.Session', ttl_seconds: int = 3600, **kwargs) -> Client:
    """
    Create a lakeFS client from an AWS role.
    :param session: : The boto3 session.
    :param ttl_seconds: The time-to-live for the generated token in seconds. The default value is 3600 seconds (1 hour).
    :param kwargs: The arguments to pass to the client.
    :return: A lakeFS client.
    """

    identity_token = get_identity_token(session)
    external_login_information = ExternalLoginInformation(token_expiration_duration=ttl_seconds, identity_request={
        "identity_token": identity_token
    })
    client = Client(**kwargs)

    with api_exception_handler():
        auth_token = client.sdk_client.auth_api.external_principal_login(external_login_information)

    client.config.access_token = auth_token.token
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
