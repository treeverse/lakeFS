"""
Client configuration module
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from lakefs_sdk import Configuration
from lakefs.exceptions import NoAuthenticationFound
from lakefs.namedtuple import LenientNamedTuple

_LAKECTL_YAML_PATH = os.path.join(Path.home(), ".lakectl.yaml")
_LAKECTL_ENDPOINT_ENV = "LAKECTL_SERVER_ENDPOINT_URL"
_LAKECTL_ACCESS_KEY_ID_ENV = "LAKECTL_CREDENTIALS_ACCESS_KEY_ID"
_LAKECTL_SECRET_ACCESS_KEY_ENV = "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"


class ClientConfig:
    """
    Configuration class for the SDK Client.
    Instantiation will try to get authentication methods using the following chain:

    1. Provided kwargs to __init__ func (should contain necessary credentials as defined in lakefs_sdk.Configuration)
    2. Use LAKECTL_SERVER_ENDPOINT_URL, LAKECTL_ACCESS_KEY_ID and LAKECTL_ACCESS_SECRET_KEY if set
    3. Try to read ~/.lakectl.yaml if exists
    4. TBD: try and use IAM role from current machine (using AWS IAM role will work only with enterprise/cloud)

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

    configuration: Configuration
    server: Server
    credentials: Credentials

    def __init__(self, **kwargs):
        self.configuration = Configuration(**kwargs)
        if kwargs:
            return

        found = False

        # Get credentials from lakectl
        try:
            with open(_LAKECTL_YAML_PATH, encoding="utf-8") as fd:
                data = yaml.load(fd, Loader=yaml.Loader)
                self.server = ClientConfig.Server(**data["server"])
                self.credentials = ClientConfig.Credentials(**data["credentials"])
            found = True
        except FileNotFoundError:  # File not found, fallback to env variables
            self.server = ClientConfig.Server(endpoint_url="")
            self.credentials = ClientConfig.Credentials(access_key_id="", secret_access_key="")

        endpoint_env = os.getenv(_LAKECTL_ENDPOINT_ENV)
        key_env = os.getenv(_LAKECTL_ACCESS_KEY_ID_ENV)
        secret_env = os.getenv(_LAKECTL_SECRET_ACCESS_KEY_ENV)

        self.configuration.host = endpoint_env if endpoint_env is not None else self.server.endpoint_url
        self.configuration.username = key_env if key_env is not None else self.credentials.access_key_id
        self.configuration.password = secret_env if secret_env is not None else self.credentials.secret_access_key
        if len(self.configuration.username) > 0 and len(self.configuration.password) > 0:
            found = True

        # TODO: authentication via IAM Role
        if not found:
            raise NoAuthenticationFound
