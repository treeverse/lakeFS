"""
Module containing lakeFS ServerConfiguration implementation
"""

from typing import NamedTuple, Optional

import lakefs_sdk

from lakefs.client import Client, DefaultClient


class ServerStorageConfiguration(NamedTuple):
    """
    Represent lakeFS's server storage configuration
    """
    blockstore_type: str
    pre_sign_support: bool
    import_support: bool


class ServerConfiguration:
    """
    Represent lakeFS's server configuration. Consists of server's storage and version configurations
    """
    _conf: lakefs_sdk.Config
    _storage_conf: ServerStorageConfiguration

    def __init__(self, client: Optional[Client] = DefaultClient):
        self._conf = client.sdk_client.config_api.get_config()
        self._storage_conf = ServerStorageConfiguration(blockstore_type=self._conf.storage_config.blockstore_type,
                                                        pre_sign_support=self._conf.storage_config.pre_sign_support,
                                                        import_support=self._conf.storage_config.import_support)

    @property
    def version(self) -> str:
        """
        Returns the current client's lakeFS version
        """
        return self._conf.version_config.version

    @property
    def storage_config(self) -> ServerStorageConfiguration:
        """
        Return lakeFS server storage configuration
        """
        return self._storage_conf
