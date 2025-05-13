"""
Client configuration module
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Optional, Dict

import yaml
from lakefs_sdk import Configuration
from lakefs.exceptions import NoAuthenticationFound, UnsupportedCredentialsProviderType
from lakefs.namedtuple import LenientNamedTuple

_LAKECTL_YAML_PATH = os.path.join(Path.home(), ".lakectl.yaml")
_LAKECTL_ENDPOINT_URL_ENV = "LAKECTL_SERVER_ENDPOINT_URL"
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
_DEFAULT_IAM_TOKEN_TTL_SECONDS = "3600"
_DEFAULT_IAM_URL_PRESIGN_TTL_SECONDS = "60"

SUPPORTED_IAM_PROVIDERS = ["aws_iam"]

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
        UNKNOWN = "unknown"

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

    server = Server(endpoint_url="")
    credentials = Credentials(access_key_id="", secret_access_key="")
    username = None
    password = None
    access_token = None
    _iam_provider = None

    def __init__(self, verify_ssl: Optional[bool] = None, proxy: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if verify_ssl is not None:
            self.verify_ssl = verify_ssl
        if proxy is not None:
            self.proxy = proxy

        if kwargs:
            return
        self._load_from_config_file()
        self._load_from_environment()
        # Check for IAM provider if no session token and no credentials
        if self.access_token is None and self.username is None and self.password is None:
            self._iam_provider = get_iam_provider_from_env_or_file(self._config_data)

        if not self._has_valid_authentication():
            raise NoAuthenticationFound

    def _load_from_config_file(self):
        """Load configuration from .lakectl.yaml file if it exists"""
        try:
            with open(_LAKECTL_YAML_PATH, encoding="utf-8") as fd:
                data = yaml.load(fd, Loader=yaml.Loader)
                if "server" in data:
                    self.server = ClientConfig.Server(**data["server"])
                if "credentials" in data:
                    try:
                        self.credentials = ClientConfig.Credentials(**data["credentials"])
                        self.username = self.credentials.access_key_id
                        self.password = self.credentials.secret_access_key
                    except TypeError:
                        pass
                self._config_data = data
        except FileNotFoundError:
            self._config_data = None

    def _load_from_environment(self):
        """Load configuration from environment variables, which take precedence"""
        endpoint_env = os.getenv(_LAKECTL_ENDPOINT_URL_ENV)
        if endpoint_env is not None:
            self.host = endpoint_env
        elif hasattr(self, 'server') and self.server:
            self.host = self.server.endpoint_url
        key_env = os.getenv(_LAKECTL_ACCESS_KEY_ID_ENV)
        secret_env = os.getenv(_LAKECTL_SECRET_ACCESS_KEY_ENV)
        if key_env is not None and secret_env is not None:
            self.username = key_env
            self.password = secret_env
            self.credentials = ClientConfig.Credentials(
                access_key_id=key_env,
                secret_access_key=secret_env
            )
        # Session token takes precedence over basic credentials
        token_env = os.getenv(_LAKECTL_CREDENTIALS_SESSION_TOKEN)
        if token_env is not None:
            self.access_token = token_env
            self.username = None
            self.password = None
            self._iam_provider = None

    def _has_valid_authentication(self) -> bool:
        """Check if we have valid authentication credentials"""
        if self.access_token is not None:
            return True
        if (self.username is not None and len(self.username) > 0 and
                self.password is not None and len(self.password) > 0):
            return True
        if self._iam_provider is not None:
            return True
        return False

    def get_auth_type(self) -> Optional[ClientConfig.AuthType]:
        """
        Returns the type of authentication used: either SessionToken, Credentials, or IAMProvider
        :return: ClientConfig.AuthType
        """
        if self._iam_provider is not None:
            return ClientConfig.AuthType.IAM
        if self.access_token is not None:
            return ClientConfig.AuthType.SESSION_TOKEN
        if self.credentials is not None:
            return ClientConfig.AuthType.CREDENTIALS
        return None

    def get_iam_provider(self) -> Optional[ClientConfig.IAMProvider]:
        """
        Returns the IAM provider used for authentication.
        :return: ClientConfig.IAMProvider
        """
        if self._iam_provider is not None:
            return self._iam_provider
        return None

def get_iam_provider_from_env_or_file(data: Optional[Dict] = None) -> Optional[ClientConfig.IAMProvider]:
    """
    Get IAM provider configuration from environment variables (primary) or config file (fallback).

    :param data: Optional config data from YAML file
    :return: IAMProvider if configured, None otherwise
    """
    provider_type = _get_provider_type(data)
    if not provider_type:
        return None

    if provider_type not in SUPPORTED_IAM_PROVIDERS:
        raise UnsupportedCredentialsProviderType(provider_type)

    if provider_type == "aws_iam":
        aws_config = _get_aws_iam_config(data)
        return ClientConfig.IAMProvider(
            type=ClientConfig.ProviderType.AWS_IAM,
            aws_iam=aws_config
        )

    return None

def _get_provider_type(data: Optional[Dict] = None) -> Optional[str]:
    """Extract provider type from environment or config data."""
    provider_type = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_TYPE)
    if provider_type:
        return provider_type
    if data and data.get("credentials", {}).get("provider", {}).get("type"):
        return data["credentials"]["provider"]["type"]

    return None

def _get_aws_iam_config(data: Optional[Dict] = None) -> ClientConfig.AWSIAMProviderConfig:
    """Build AWS IAM provider configuration from environment and config file."""
    env_token_ttl = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS)
    env_url_ttl = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_URL_PRESIGN_TTL_SECONDS)
    env_headers = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS)

    # Default values
    token_ttl_seconds = int(_DEFAULT_IAM_TOKEN_TTL_SECONDS)
    url_presign_ttl_seconds = int(_DEFAULT_IAM_URL_PRESIGN_TTL_SECONDS)
    token_request_headers = None

    if (data and "credentials" in data and
            "provider" in data["credentials"] and
            "aws_iam" in data["credentials"]["provider"]):
        file_config = data["credentials"]["provider"]["aws_iam"]
        if file_config:
            token_ttl_seconds = _safe_int_or_default(file_config.get("token_ttl_seconds"), token_ttl_seconds)
            url_presign_ttl_seconds = _safe_int_or_default(
                file_config.get("url_presign_ttl_seconds"), url_presign_ttl_seconds)
            if "token_request_headers" in file_config:
                token_request_headers = file_config["token_request_headers"]

    # Environment variables override
    token_ttl_seconds = _safe_int_or_default(env_token_ttl, token_ttl_seconds)
    url_presign_ttl_seconds = _safe_int_or_default(env_url_ttl, url_presign_ttl_seconds)

    if env_headers is not None:
        token_request_headers = env_headers

    return ClientConfig.AWSIAMProviderConfig(
        token_ttl_seconds=token_ttl_seconds,
        url_presign_ttl_seconds=url_presign_ttl_seconds,
        token_request_headers=token_request_headers
    )

def _safe_int_or_default(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default
