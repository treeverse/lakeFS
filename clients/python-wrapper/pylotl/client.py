import base64
import binascii
import copy
from typing import Optional
import httpx

import lakefs_sdk
from lakefs_sdk.client import LakeFSClient
from pylotl.config import Config
from pylotl.exceptions import NoAuthenticationFound, UnsupportedOperationException


class Client:
    """
    Wrapper around lakefs_sdk's client object
    Takes care of instantiating it from the environment
    """

    _client: LakeFSClient
    _http_client: httpx.Client
    _conf: Config
    _storage_conf: lakefs_sdk.StorageConfig = None

    def __init__(self, **kwargs):
        self._conf = Config(**kwargs)
        self._client = LakeFSClient(self._conf.get_config())

        # Set up http client
        config = self._conf.get_config()
        headers = {}
        auth = None
        if config.access_token is not None:
            headers["Authorization"] = f"Bearer {config.access_token}"

        if config.username is not None and config.password is not None:
            auth = httpx.BasicAuth(config.username, config.password)

        self._http_client = httpx.Client(headers=headers, auth=auth)

    def __del__(self):
        self._http_client.close()

    @property
    def config(self):
        return copy.deepcopy(self._conf.get_config())

    @property
    def sdk_client(self):
        return self._client

    @property
    def storage_config(self):
        if self._storage_conf is None:
            self._storage_conf = self._client.internal_api.get_storage_config()
        return copy.deepcopy(self._storage_conf)

    @staticmethod
    def _extract_etag_from_response(headers) -> str:
        # prefer Content-MD5 if exists
        content_md5 = headers.get("Content-MD5")
        if content_md5 is not None and len(content_md5) > 0:
            try:  # decode base64, return as hex
                decode_md5 = base64.b64decode(content_md5)
                return binascii.hexlify(decode_md5).decode("utf-8")
            except binascii.Error:
                pass

        # fallback to ETag
        etag = headers.get("ETag", "").strip(' "')
        return etag

    # TODO: Consider moving under WriteableObject
    def upload(self, repo, ref, path, content, pre_sign, content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None) -> lakefs_sdk.ObjectStats:
        if not pre_sign:
            raise UnsupportedOperationException("Upload currently supported only in pre-sign mode")

        headers = {}
        if content_type is not None:
            headers["Content-Type"] = content_type

        staging_location = self._client.staging_api.get_physical_address(repo, ref, path, pre_sign)
        url = staging_location.presigned_url
        if self.storage_config.blockstore_type == "azure":
            headers["x-ms-blob-type"] = "BlockBlob"

        resp = self._http_client.put(url,
                                     content=content,
                                     headers=headers,
                                     auth=None)  # Explicitly remove default client authentication
        resp.raise_for_status()
        etag = Client._extract_etag_from_response(resp.headers)
        staging_metadata = lakefs_sdk.StagingMetadata(staging=staging_location,
                                                      size_bytes=len(content),
                                                      checksum=etag,
                                                      user_metadata=metadata)
        return self._client.staging_api.link_physical_address(repo, ref, path, staging_metadata=staging_metadata)


# global default client
DefaultClient: Optional[Client] = None

try:
    DefaultClient = Client()
except NoAuthenticationFound:
    # must call init() explicitly
    DefaultClient = None


def init(**kwargs):
    global DefaultClient
    DefaultClient = Client(**kwargs)
