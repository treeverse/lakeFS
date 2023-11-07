import os
import viper
from lakefs_sdk import (
    Configuration,
)
from pylotl.exceptions import NoAuthenticationFound

_LAKECTL_YAML_PATH = os.path.expanduser(os.path.join("~", ".lakectl.yaml"))
_LAKECTL_ENDPOINT_ENV = "LAKECTL_SERVER_ENDPOINT_URL"
_LAKECTL_ACCESS_KEY_ID_ENV = "LAKECTL_CREDENTIALS_ACCESS_KEY_ID"
_LAKECTL_SECRET_ACCESS_KEY_ENV = "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"


class ClientConfig:
    """
    Configuration class for the SDK wrapper Client.
    Instantiation will try to get authentication methods using the following chain:
    1. Provided kwargs to __init__ func (should contain necessary credentials as defined in lakefs_sdk.Configuration)
    2. Use LAKECTL_SERVER_ENDPOINT_URL, LAKECTL_ACCESS_KEY_ID and LAKECTL_ACCESS_SECRET_KEY if set
    3. Try to read ~/.lakectl.yaml if exists
    4. TBD: try and use IAM role from current machine (using AWS IAM role will work only with enterprise/cloud)

    This class also encapsulates the required lakectl configuration for authentication and used to unmarshall the 
    lakectl yaml file.
    """

    class Server:
        endpoint_url = ""

    class Credentials:
        access_key_id = ""
        secret_access_key = ""

    _configuration: Configuration
    server = Server
    credentials = Credentials

    def __init__(self, **kwargs):
        self._configuration = Configuration(**kwargs)
        if kwargs:
            return

        found = False
        # Get credentials from lakectl
        viper.set_config_path(_LAKECTL_YAML_PATH)
        try:
            viper.read_config()
            viper.unmarshal(self)
            found = True
        except FileNotFoundError:  # File not found, fallback to env variables
            pass

        endpoint_env = os.getenv(_LAKECTL_ENDPOINT_ENV)
        key_env = os.getenv(_LAKECTL_ACCESS_KEY_ID_ENV)
        secret_env = os.getenv(_LAKECTL_SECRET_ACCESS_KEY_ENV)

        self._configuration.host = endpoint_env or self.server.endpoint_url
        self._configuration.username = key_env or self.credentials.access_key_id
        self._configuration.password = secret_env or self.credentials.secret_access_key
        if len(self._configuration.username) > 0 and len(self._configuration.password) > 0:
            found = True

        # TODO: authentication via IAM Role
        if not found:
            raise NoAuthenticationFound

    def get_config(self):
        return self._configuration
