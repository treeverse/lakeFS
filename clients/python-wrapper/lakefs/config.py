"""
Client configuration module
"""

from __future__ import annotations

import dataclasses
import json
import os
from enum import Enum
from json import JSONDecodeError
from pathlib import Path
from typing import Optional, Dict

import yaml
from lakefs_sdk import Configuration
from lakefs.exceptions import NoAuthenticationFound, UnsupportedCredentialsProviderType, InvalidEnvVarFormat
from lakefs.namedtuple import LenientNamedTuple

_LAKECTL_YAML_PATH = os.path.join(Path.home(), ".lakectl.yaml")
_LAKECTL_ENDPOINT_URL_ENV = "LAKECTL_SERVER_ENDPOINT_URL"
_LAKECTL_ACCESS_KEY_ID_ENV = "LAKECTL_CREDENTIALS_ACCESS_KEY_ID"
_LAKECTL_SECRET_ACCESS_KEY_ENV = "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"
# lakefs access token, used for authentication when logging in with an IAM role
_LAKECTL_CREDENTIALS_SESSION_TOKEN = "LAKECTL_CREDENTIALS_SESSION_TOKEN"
_LAKECTL_CREDENTIALS_PROVIDER_TYPE = "LAKECTL_CREDENTIALS_PROVIDER_TYPE"
_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS = "LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS"
_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_PRESIGNED_URL_TTL_SECONDS = \
    "LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_PRESIGNED_URL_TTL_SECONDS"
_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS = \
    "LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS"

# Defaults
_DEFAULT_IAM_TOKEN_TTL_SECONDS = 3600
_DEFAULT_IAM_URL_PRESIGN_TTL_SECONDS = 60

TOKEN_TTL_SECONDS_CONFIG = "token_ttl_seconds"
URL_PRESIGN_TTL_SECONDS_CONFIG = "url_presign_ttl_seconds"
TOKEN_REQUEST_HEADERS_CONFIG = "token_request_headers"

AWS_IAM_PROVIDER_TYPE = "aws_iam"
SUPPORTED_IAM_PROVIDERS = [AWS_IAM_PROVIDER_TYPE]

class ClientConfig(Configuration):
    """
    Configuration class for the SDK Client.
    Instantiation will try to get authentication methods using the following chain:

    1. Provided kwargs to __init__ func (should contain necessary credentials as defined in lakefs_sdk.Configuration)
    2. Use LAKECTL_SERVER_ENDPOINT_URL, LAKECTL_ACCESS_KEY_ID and LAKECTL_ACCESS_SECRET_KEY if set
    3. Try to read ~/.lakectl.yaml if exists
    4. Use IAM role from current machine (using AWS IAM role will work only with enterprise/cloud)

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

    @dataclasses.dataclass
    class AWSIAMProviderConfig:
        """
        lakectl configuration's credentials block
        """
        token_ttl_seconds: int
        url_presign_ttl_seconds: int
        token_request_headers: Optional[Dict]

    @dataclasses.dataclass
    class IAMProvider:
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

        if not self._has_valid_authentication():
            raise NoAuthenticationFound

    def _load_from_config_file(self):
        """Load configuration from .lakectl.yaml file if it exists"""
        try:
            with open(_LAKECTL_YAML_PATH, encoding="utf-8") as fd:
                config_data = yaml.load(fd, Loader=yaml.Loader)
                if "server" in config_data:
                    self.server = ClientConfig.Server(**config_data["server"])
                if "credentials" in config_data:
                    if ("access_key_id" in config_data["credentials"]
                        and "secret_access_key" in config_data["credentials"]):
                        self.credentials = ClientConfig.Credentials(**config_data["credentials"])
                        self.username = self.credentials.access_key_id
                        self.password = self.credentials.secret_access_key

                if self.username is None or self.password is None:
                    self._set_iam_provider_from_config_file(config_data)
        except FileNotFoundError:
            pass


    def _load_from_environment(self):
        """Load configuration from environment variables, which take precedence"""
        endpoint_env = os.getenv(_LAKECTL_ENDPOINT_URL_ENV)
        if endpoint_env is not None:
            self.host = endpoint_env
        elif hasattr(self, 'server') and self.server:
            self.host = self.server.endpoint_url
        # Session token takes precedence over basic credentials and IAM provider. If specified, set it and override
        # all others. Currently, session token setting is only available through environment variables.
        token_env = os.getenv(_LAKECTL_CREDENTIALS_SESSION_TOKEN)
        if token_env is not None:
            self.access_token = token_env
            self.username = None
            self.password = None
            self.credentials = None
            self._iam_provider = None
            return
        # Credentials take precedence over IAM provider. If specified, set it and override the IAM provider
        key_env = os.getenv(_LAKECTL_ACCESS_KEY_ID_ENV)
        secret_env = os.getenv(_LAKECTL_SECRET_ACCESS_KEY_ENV)
        if key_env is not None and secret_env is not None:
            self.username = key_env
            self.password = secret_env
            self.credentials = ClientConfig.Credentials(
                access_key_id=key_env,
                secret_access_key=secret_env
            )
            self._iam_provider = None
            return
        # If no other method was specified, try to set IAM provider from env vars
        if self.username is None or self.password is None:
            self._set_iam_provider_from_env_vars()



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
        ORDER MATTERS! SessionToken > Credentials > IAMProvider. self._iam_provider will be none if Session Token auth
        is used. self.access_token will be populated for both Session Token and IAMProvider auth, therefore it's tested
        after self._iam_provider.
        :return: ClientConfig.AuthType
        """
        if self._iam_provider is not None:
            return ClientConfig.AuthType.IAM
        if self.access_token is not None:
            return ClientConfig.AuthType.SESSION_TOKEN
        if self.credentials is not None:
            return ClientConfig.AuthType.CREDENTIALS
        return None

    @property
    def iam_provider(self) -> Optional[ClientConfig.IAMProvider]:
        """
        Returns the IAM provider used for authentication.
        :return: ClientConfig.IAMProvider
        """
        return self._iam_provider

    def _set_iam_provider_from_config_file(self, config_data: Dict):
        """
        Set the IAM provider from the configuration file.
        """
        provider_type = _get_provider_type_from_config_file(config_data)
        if provider_type is None:
            self._iam_provider = None
        elif provider_type not in SUPPORTED_IAM_PROVIDERS:
            raise UnsupportedCredentialsProviderType(provider_type)
        else:
            provider_config = _get_provider_config_from_config_data(config_data, provider_type)
            if provider_config is not None:
                if provider_type == AWS_IAM_PROVIDER_TYPE:
                    aws_iam_provider_config = _generate_aws_iam_provider_config(provider_config)
                    self._iam_provider = ClientConfig.IAMProvider(
                        type=ClientConfig.ProviderType.AWS_IAM,
                        aws_iam=aws_iam_provider_config
                    )

    def _set_iam_provider_from_env_vars(self):
        """
        Set the IAM provider from environment variables.
        """
        provider_type = _get_iam_provider_type_from_env_vars()
        if provider_type is None:
            return
        if provider_type == AWS_IAM_PROVIDER_TYPE:
            if self._iam_provider is None:
                self._iam_provider = ClientConfig.IAMProvider(
                    type=ClientConfig.ProviderType.AWS_IAM,
                    aws_iam=ClientConfig.AWSIAMProviderConfig(
                        token_ttl_seconds=_DEFAULT_IAM_TOKEN_TTL_SECONDS,
                        url_presign_ttl_seconds=_DEFAULT_IAM_URL_PRESIGN_TTL_SECONDS,
                        token_request_headers=None
                    )
                )
            env_token_ttl = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS,
                                      self._iam_provider.aws_iam.token_ttl_seconds)
            self._iam_provider.aws_iam.token_ttl_seconds = int(env_token_ttl)
            env_presign_url_ttl = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_PRESIGNED_URL_TTL_SECONDS,
                                            self._iam_provider.aws_iam.url_presign_ttl_seconds)
            self._iam_provider.aws_iam.url_presign_ttl_seconds= int(env_presign_url_ttl)
            env_headers = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS, None)
            if env_headers is not None:
                try:
                    token_request_headers = json.loads(env_headers)
                    self._iam_provider.aws_iam.token_request_headers = token_request_headers
                except JSONDecodeError as e:
                    raise InvalidEnvVarFormat(
                        f"Invalid format for {env_headers} environment variable. Expected JSON format."
                    ) from e
        else:
            raise UnsupportedCredentialsProviderType(provider_type)

def _get_provider_type_from_config_file(data: Optional[Dict] = None) -> Optional[str]:
    """Extract provider type from environment or config data."""
    if data is not None:
        try:
            return data['credentials']['provider']['type']
        except KeyError:
            return None
    return None

def _get_iam_provider_type_from_env_vars() -> Optional[str]:
    """
    Get IAM provider configuration from environment variables.

    :return: IAMProvider if configured, None otherwise
    """
    provider_type = os.getenv(_LAKECTL_CREDENTIALS_PROVIDER_TYPE)
    if provider_type is not None and provider_type not in SUPPORTED_IAM_PROVIDERS:
        raise UnsupportedCredentialsProviderType(provider_type)
    return provider_type


def _safe_int_or_default(value: Optional[str], default: int) -> int:
    """
    Safely convert a value to an int, returning a default if conversion fails.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def _get_provider_config_from_config_data(data: Optional[Dict], provider: str) -> Optional[Dict]:
    if data is not None:
        try:
            return data['credentials']['provider'][provider]
        except KeyError:
            return None
    return None

def _generate_aws_iam_provider_config(aws_config: Dict) -> ClientConfig.AWSIAMProviderConfig:
    token_ttl_seconds = _safe_int_or_default(aws_config.get(TOKEN_TTL_SECONDS_CONFIG), _DEFAULT_IAM_TOKEN_TTL_SECONDS)
    url_presign_ttl_seconds = _safe_int_or_default(
        aws_config.get(URL_PRESIGN_TTL_SECONDS_CONFIG), _DEFAULT_IAM_URL_PRESIGN_TTL_SECONDS)
    token_request_headers = None
    if TOKEN_REQUEST_HEADERS_CONFIG in aws_config:
        token_request_headers = aws_config[TOKEN_REQUEST_HEADERS_CONFIG]
    return ClientConfig.AWSIAMProviderConfig(
            token_ttl_seconds=token_ttl_seconds,
            url_presign_ttl_seconds=url_presign_ttl_seconds,
            token_request_headers=token_request_headers
    )
