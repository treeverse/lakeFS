"""
Client configuration module
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from lakefs_sdk import Configuration
from lakefs.exceptions import NoAuthenticationFound, UnsupportedCredentialsProviderType
from lakefs.namedtuple import LenientNamedTuple

_LAKECTL_YAML_PATH = os.path.join(Path.home(), ".lakectl.yaml")
_LAKECTL_ENDPOINT_ENV = "LAKECTL_SERVER_ENDPOINT_URL"
_LAKECTL_ACCESS_KEY_ID_ENV = "LAKECTL_CREDENTIALS_ACCESS_KEY_ID"
_LAKECTL_SECRET_ACCESS_KEY_ENV = "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"
# lakefs access token, used for authentication when logging in with an IAM role
_LAKECTL_CREDENTIALS_SESSION_TOKEN = "LAKECTL_CREDENTIALS_SESSION_TOKEN"
_LAKECTL_CREDENTIALS_PROVIDER_TYPE = "LAKECTL_CREDENTIALS_PROVIDER_TYPE"
_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS = "LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS"
_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_URL_PRESIGN_TTL_SECONDS = \
    "LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_URL_PRESIGN_TTL_SECONDS"
_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS = \
    "LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS"

# Defaults
_DEFAULT_AWS_IAM_TOKEN_TTL_SECONDS = "3600"
_DEFAULT_AWS_IAM_URL_PRESIGN_TTL_SECONDS = "60"


def parse_credentials_provider(data) -> Optional[ClientConfig.IAMProvider]:
    """
    Parse the credentials provider string into the corresponding IAMProvider.
    :param data: config yaml representation
    :return: The corresponding ProviderType enum, or None if not found.
    """
    provider_type: Optional[str] = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_TYPE, None)
    if provider_type is None and "credentials" in data and "provider" in data["credentials"]:
        provider = data["credentials"]["provider"]
        provider_type = provider["type"]
    if provider_type is not None:
        if provider_type == "aws_iam":
            aws_iam = provider["aws_iam"]
            token_ttl_seconds = _DEFAULT_AWS_IAM_TOKEN_TTL_SECONDS
            url_presign_ttl_seconds = _DEFAULT_AWS_IAM_URL_PRESIGN_TTL_SECONDS
            token_request_headers = None
            if "token_ttl_seconds" in aws_iam:
                token_ttl_seconds = int(os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS,
                                                  aws_iam["token_ttl_seconds"]))
            if "url_presign_ttl_seconds" in aws_iam:
                url_presign_ttl_seconds = int(
                    os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_URL_PRESIGN_TTL_SECONDS,
                              aws_iam["url_presign_ttl_seconds"])
                )
            if "token_request_headers" in aws_iam:
                token_request_headers = dict(
                    os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS,
                              aws_iam["token_request_headers"])
                )
            iam = ClientConfig.AWSIAMProviderConfig(
                token_ttl_seconds=token_ttl_seconds,
                url_presign_ttl_seconds=url_presign_ttl_seconds,
                token_request_headers=token_request_headers
            )
            return ClientConfig.IAMProvider(
                type=ClientConfig.ProviderType.AWS_IAM,
                aws_iam=iam
            )
        else:
            raise UnsupportedCredentialsProviderType(provider_type)
    return None


class ClientConfig(Configuration):
    """
    Configuration class for the SDK Client.
    Instantiation will try to get authentication methods using the following chain:

    1. Provided kwargs to __init__ func (should contain necessary credentials as defined in lakefs_sdk.Configuration)
    2. Use LAKECTL_SERVER_ENDPOINT_URL, LAKECTL_ACCESS_KEY_ID and LAKECTL_ACCESS_SECRET_KEY if set
    3. Try to read ~/.lakectl.yaml if exists
    4. Use IAM role from current machine (using AWS IAM role will work only with enterprise/cloud)
    5. If the credentials provider type is set to aws_iam, use the credentials from the machine's AWS profile

    This class also encapsulates the required lakectl configuration for authentication and used to unmarshall the
    lakectl yaml file.
    """

    class Server(LenientNamedTuple):
        """
        lakectl configuration's server block
        """
        endpoint_url: str

    class Credentials(LenientNamedTuple):
        """
        lakectl configuration's credentials block
        """
        access_key_id: str
        secret_access_key: str

    class AuthType(Enum):
        """
        Enum for the supported authentication types
        """
        SESSION_TOKEN = 1
        CREDENTIALS = 2
        IAM = 3

    class ProviderType(Enum):
        """
        Enum for the supported authentication provider types
        """
        AWS_IAM = "aws_iam"
        Unknown = "unknown"

    class AWSIAMProviderConfig(LenientNamedTuple):
        """
        lakectl configuration's credentials block
        """
        token_ttl_seconds: int
        url_presign_ttl_seconds: int
        token_request_headers: dict[str, str]

    class IAMProvider(LenientNamedTuple):
        """
        An IAM authentication provider
        """
        type: ClientConfig.ProviderType
        aws_iam: Optional[ClientConfig.AWSIAMProviderConfig]

    server: Server
    credentials: Credentials
    _iam_provider: Optional[IAMProvider] = None

    def __init__(self, verify_ssl: Optional[bool] = None, proxy: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if verify_ssl is not None:
            self.verify_ssl = verify_ssl
        if proxy is not None:
            self.proxy = proxy

        if kwargs:
            return

        found = False

        # Get credentials from lakectl
        try:
            with open(_LAKECTL_YAML_PATH, encoding="utf-8") as fd:
                data = yaml.load(fd, Loader=yaml.Loader)
                self.server = (ClientConfig.Server(**data["server"])
                               if "server" in data
                               else ClientConfig.Server(endpoint_url=""))
                try:
                    self.credentials = (ClientConfig.Credentials(**data["credentials"])
                                   if "credentials" in data
                                   else ClientConfig.Credentials(access_key_id="", secret_access_key=""))
                    self._iam_provider = ClientConfig.IAMProvider(type=ClientConfig.ProviderType.Unknown, aws_iam=None)
                except TypeError:
                    print("Failed to parse access key and secret from lakectl yaml file.\nTrying IAM provider...")
                    self.credentials = ClientConfig.Credentials(access_key_id="", secret_access_key="")
                    self._iam_provider = parse_credentials_provider(data)
                found = True
        except FileNotFoundError:  # File not found, fallback to env variables
            self.server = ClientConfig.Server(endpoint_url="")
            self.credentials = ClientConfig.Credentials(access_key_id="", secret_access_key="")

        endpoint_env = os.getenv(_LAKECTL_ENDPOINT_ENV)
        key_env = os.getenv(_LAKECTL_ACCESS_KEY_ID_ENV)
        secret_env = os.getenv(_LAKECTL_SECRET_ACCESS_KEY_ENV)

        if key_env is not None or secret_env is not None:
            self.username = key_env
            self.credentials.access_key_id = key_env
        if secret_env is not None:
            self.password = secret_env
            self.credentials.secret_access_key = secret_env
        self.host = endpoint_env if endpoint_env is not None else self.server.endpoint_url
        self.access_token = os.getenv(_LAKECTL_CREDENTIALS_SESSION_TOKEN)

        if (self.access_token is not None or
                ((self.username is not None and len(self.username) > 0) and (self.password is not None and len(self.password) > 0)) or
                self._iam_provider is not None):
            found = True

        if not found:
                raise NoAuthenticationFound

    def get_auth_type(self) -> Optional[AuthType]:
        """
        Returns the type of authentication used: either SessionToken, Credentials, or IAMProvider
        :return: ClientConfig.AuthType
        """
        if self._iam_provider is not None:
            return ClientConfig.AuthType.IAM
        if self.access_token is not None:
            return ClientConfig.AuthType.SESSION_TOKEN
        if self.username is not None and self.password is not None:
            return ClientConfig.AuthType.CREDENTIALS
        return None

    def get_iam_provider(self) -> Optional[IAMProvider]:
        """
        Returns the IAM provider used for authentication.
        :return: ClientConfig.IAMProvider
        """
        if self._iam_provider is not None:
            return self._iam_provider
        return None
