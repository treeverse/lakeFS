"""
lakeFS Client module

Handles authentication against the lakeFS server and wraps the underlying lakefs_sdk client.
"""

from __future__ import annotations

import base64
import json
from threading import Lock
from typing import Optional
from typing import TYPE_CHECKING
from urllib.parse import urlparse, parse_qs

import lakefs_sdk
from lakefs_sdk import ExternalLoginInformation
from lakefs_sdk.client import LakeFSClient

from lakefs.config import ClientConfig
from lakefs.exceptions import NotAuthorizedException, ServerException, api_exception_handler
from lakefs.models import ServerStorageConfiguration

if TYPE_CHECKING:
    import boto3

DEFAULT_REGION = 'us-east-1'


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


def _extract_region_from_endpoint(endpoint):
    """
    Extract the region name from an STS endpoint URL.
    for example: https://sts.eu-central-1.amazonaws.com/ -> eu-central-1
    and for example: https://sts.amazonaws.com/ -> DEFAULT_REGION

    :param endpoint: The endpoint URL of the STS client.
    :return: The region name extracted from the endpoint URL.
    """

    parts = endpoint.split('.')
    if len(parts) == 4:
        return parts[1]
    if len(parts) > 4:
        return parts[2]
    return DEFAULT_REGION


def _get_identity_token(
        session: boto3.Session,
        lakefs_host: str,
        additional_headers: dict[str, str],
        presign_expiry
) -> str:
    """
   Generate the identity token required for lakeFS authentication from an AWS session.

   This function uses the STS client to generate a presigned URL for the `get_caller_identity` action,
    extracts the required values from the URL,
   and creates a base64-encoded JSON object with these values.

   :param session: A boto3 session object with the necessary AWS credentials and region information.
   :return: A base64-encoded JSON string containing the required authentication information.
   :raises ValueError: If the session does not have a region name set.
   """

    # this method should only be called when installing the aws-iam additional requirement
    from botocore.client import Config  # pylint: disable=import-outside-toplevel, import-error
    from botocore.signers import RequestSigner  # pylint: disable=import-outside-toplevel, import-error

    sts_client = session.client('sts', config=Config(signature_version='v4'))
    endpoint = sts_client.meta.endpoint_url
    service_id = sts_client.meta.service_model.service_id
    region = _extract_region_from_endpoint(endpoint)
    # signer is used because the presigned URL generated by the STS does not support additional headers
    signer = RequestSigner(
        service_id,
        region,
        'sts',
        'v4',
        session.get_credentials(),
        session.events
    )
    endpoint_with_params = f"{endpoint}/?Action=GetCallerIdentity&Version=2011-06-15"
    if additional_headers is None:
        additional_headers = {
            'X-LakeFS-Server-ID': lakefs_host,
        }
    params = {
        'method': 'POST',
        'url': endpoint_with_params,
        'body': {},
        'headers': additional_headers,
        'context': {}
    }

    presigned_url = signer.generate_presigned_url(
        params,
        region_name=region,
        expires_in=presign_expiry,
        operation_name=''
    )
    parsed_url = urlparse(presigned_url)
    query_params = parse_qs(parsed_url.query)

    # Extract values from query parameters
    json_object = {
        "method": "POST",
        "host": parsed_url.hostname,
        "region": region,
        "action": query_params['Action'][0],
        "date": query_params['X-Amz-Date'][0],
        "expiration_duration": query_params['X-Amz-Expires'][0],
        "access_key_id": query_params['X-Amz-Credential'][0].split('/')[0],
        "signature": query_params['X-Amz-Signature'][0],
        "signed_headers": query_params.get('X-Amz-SignedHeaders', [''])[0].split(';'),
        "version": query_params['Version'][0],
        "algorithm": query_params['X-Amz-Algorithm'][0],
        "security_token": query_params.get('X-Amz-Security-Token', [None])[0]
    }

    json_string = json.dumps(json_object)
    return base64.b64encode(json_string.encode('utf-8')).decode('utf-8')


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
    identity_token = _get_identity_token(session, lakefs_host, presign_expiry=presigned_ttl,
                                         additional_headers=additional_headers)
    external_login_information = ExternalLoginInformation(token_expiration_duration=ttl_seconds, identity_request={
        "identity_token": identity_token
    })

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
