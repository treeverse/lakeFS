"""
Basic object IO implementation providing a filesystem like behavior over lakeFS 
"""

import base64
import binascii
from typing import Optional, Literal, Union, Iterable, AsyncIterable, NamedTuple

from lakefs_sdk import StagingMetadata
from lakefs.client import Client, DefaultClient
from lakefs.exceptions import (
    ObjectExistsException,
    ObjectNotFoundException,
    PermissionException,
    LakeFSException,
    api_exception_handler,
    NotFoundException,
    NotAuthorizedException,
    ForbiddenException
)

_RANGE_STR_TMPL = "bytes={start}-{end}"
_LAKEFS_METADATA_PREFIX = "x-lakefs-meta-"

# Type to support both strings and bytes in addition to streams (reference: httpx._types.RequestContent)
UploadContentType = Union[str, bytes, Iterable[bytes], AsyncIterable[bytes]]


class ObjectStats(NamedTuple):
    """
    Represent a lakeFS object's stats
    """
    path: str
    path_type: str
    physical_address: str
    checksum: str
    mtime: int
    physical_address_expiry: Optional[int] = None
    size_bytes: Optional[int] = None
    metadata: Optional[dict[str, str]] = None
    content_type: Optional[str] = None


class ReadableObject:
    """
    ReadableObject provides read-only functionality for lakeFS objects with IO semantics.
    This Object is instantiated and returned on open() methods for immutable reference types (Commit, Tag...)
    """
    _client: Client
    _repo: str
    _ref: str
    _path: str
    _pos: int
    _pre_sign: Optional[bool] = None
    _stat: Optional[ObjectStats] = None

    def __init__(self, repository: str, reference: str, path: str,
                 pre_sign: Optional[bool] = None, client: Optional[Client] = DefaultClient) -> None:
        self._client = client
        self._repo = repository
        self._ref = reference
        self._path = path
        self._pre_sign = pre_sign
        self._pos = 0

    @property
    def path(self) -> str:
        """
        Returns the object's path relative to repository and reference ids 
        """
        return self._path

    @property
    def pre_sign(self) -> bool:
        """
        Returns the default pre_sign mode for this object. If pre_sign was not defined during initialization, will
        take the server pre_sign capability.
        """
        if self._pre_sign is None:
            self._pre_sign = self._client.storage_config.pre_sign_support

        return self._pre_sign

    def tell(self) -> int:
        """
        Object's read position. If position is past object byte size, trying to read the object will return EOF
        """
        return self._pos

    def seek(self, pos) -> None:
        """
        Move the object's reading position

        :param pos: The position (in bytes) to move to
        :raises OSError: If provided position is negative
        """
        if pos < 0:
            raise OSError("position must be a non-negative integer")
        self._pos = pos

    def read(self, read_bytes: int = None) -> bytes:
        """
        Read object data

        :param read_bytes: How many bytes to read. If read_bytes is None, will read from current position to end.
            If current position + read_bytes > object size.
        :return: The bytes read
        :raises:
            EOFError if current position is after object size
            OSError if read_bytes is non-positive
        """
        if read_bytes and read_bytes <= 0:
            raise OSError("read_bytes must be a positive integer")

        stat = self.stat()
        if self._pos >= stat.size_bytes:
            raise EOFError
        read_bytes = read_bytes if read_bytes is not None else stat.size_bytes
        new_pos = min(self._pos + read_bytes, stat.size_bytes)
        read_range = _RANGE_STR_TMPL.format(start=self._pos, end=new_pos - 1)
        contents = self._client.sdk_client.objects_api.get_object(self._repo,
                                                                  self._ref,
                                                                  self._path,
                                                                  range=read_range,
                                                                  presign=self.pre_sign)
        self._pos = new_pos  # Update pointer position
        return contents

    def stat(self) -> ObjectStats:
        """
        Return the Stat object representing this object
        """
        if self._stat is None:
            with api_exception_handler(_io_exception_handler):
                stat = self._client.sdk_client.objects_api.stat_object(self._repo, self._ref, self._path)
                self._stat = ObjectStats(**stat.__dict__)
        return self._stat

    def exists(self) -> bool:
        """
        Returns True if object exists in lakeFS, False otherwise
        """
        exists = False

        def exist_handler(e: LakeFSException):
            if isinstance(e, NotFoundException):
                return None  # exists = False
            return _io_exception_handler(e)

        with api_exception_handler(exist_handler):
            self._client.sdk_client.objects_api.head_object(self._repo, self._ref, self._path)
            exists = True

        return exists


class WriteableObject(ReadableObject):
    """
    WriteableObject inherits from ReadableObject and provides read/write functionality for lakeFS objects
    using IO semantics.
    This Object is instantiated and returned upon invoking open() on Branch reference type.
    """

    def __init__(self, repository: str, reference: str, path: str,
                 pre_sign: Optional[bool] = None, client: Optional[Client] = DefaultClient) -> None:
        super().__init__(repository, reference, path, pre_sign, client=client)

    def create(self,
               data: UploadContentType,
               mode: Literal['x', 'xb', 'w', 'wb'] = 'wb',
               pre_sign: Optional[bool] = None,
               content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None) -> ObjectStats:
        """
        Creates a new object or overwrites an existing object

        :param data: The contents of the object to write (can be bytes or string)
        :param mode: Write modes:
            'x'     - Open for exclusive creation
            'xb'    - Open for exclusive creation in binary mode
            'w'     - Create a new object or truncate if exists
            'wb'    - Create or truncate in binary mode
        :param pre_sign: (Optional) Explicitly state whether to use pre_sign mode when uploading the object.
            If None, will be taken from pre_sign property.
        :param content_type: (Optional) Explicitly set the object Content-Type
        :param metadata: (Optional) User metadata
        :return: The Stat object representing the newly created object
        :raises:
            ObjectExistsException if object exists and mode is exclusive ('x')
        """
        content = data
        if 'x' in mode and self.exists():  # Requires explicit create
            raise ObjectExistsException

        # TODO: handle streams
        binary_mode = 'b' in mode
        if binary_mode and isinstance(data, str):
            content = data.encode('utf-8')
        elif not binary_mode and isinstance(data, bytes):
            content = data.decode('utf-8')
        is_presign = pre_sign if pre_sign is not None else self.pre_sign

        with api_exception_handler(_io_exception_handler):
            stats = self._upload_presign(content, content_type, metadata) if is_presign \
                else self._upload_raw(content, content_type, metadata)
            self._stat = ObjectStats(**stats.__dict__)

        # reset pos after create
        self._pos = 0
        return self._stat

    def delete(self) -> None:
        """
        Delete object from lakeFS
        """
        with api_exception_handler(_io_exception_handler):
            return self._client.sdk_client.objects_api.delete_object(self._repo, self._ref, self._path)

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

    def _upload_raw(self, content: UploadContentType, content_type: Optional[str] = None,
                    metadata: Optional[dict[str, str]] = None) -> ObjectStats:
        """
        Use raw upload API call to bypass validation of content parameter
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": content_type if content_type is not None else "application/octet-stream"
        }

        # Create user metadata headers
        for k, v in metadata.items():
            headers[_LAKEFS_METADATA_PREFIX + k] = v

        _response_types_map = {
            '201': "ObjectStats",
            '400': "Error",
            '401': "Error",
            '403': "Error",
            '404': "Error",
            '412': "Error",
            '420': None,
        }
        # authentication setting
        _auth_settings = ['basic_auth', 'cookie_auth', 'oidc_auth', 'saml_auth', 'jwt_token']
        return self._client.sdk_client.objects_api.api_client.call_api(
            resource_path='/repositories/{repository}/branches/{branch}/objects',
            method='POST',
            path_params={"repository": self._repo, "branch": self._ref},
            query_params={"path": self._path},
            header_params=headers,
            body=content,
            response_types_map=_response_types_map,
            auth_settings=_auth_settings,
            _return_http_data_only=True
        )

    def _upload_presign(self, content: UploadContentType, content_type: Optional[str] = None,
                        metadata: Optional[dict[str, str]] = None) -> ObjectStats:
        headers = {
            "Accept": "application/json",
            "Content-Type": content_type if content_type is not None else "application/octet-stream"
        }

        staging_location = self._client.sdk_client.staging_api.get_physical_address(self._repo,
                                                                                    self._ref,
                                                                                    self._path,
                                                                                    True)
        url = staging_location.presigned_url
        if self._client.storage_config.blockstore_type == "azure":
            headers["x-ms-blob-type"] = "BlockBlob"

        resp = self._client.sdk_client.objects_api.api_client.request(
            method="PUT",
            url=url,
            body=content,
            headers=headers
        )

        etag = WriteableObject._extract_etag_from_response(resp.getheaders())
        staging_metadata = StagingMetadata(staging=staging_location,
                                           size_bytes=len(content),
                                           checksum=etag,
                                           user_metadata=metadata)
        return self._client.sdk_client.staging_api.link_physical_address(self._repo, self._ref, self._path,
                                                                         staging_metadata=staging_metadata)


def _io_exception_handler(e: LakeFSException):
    if isinstance(e, NotFoundException):
        return ObjectNotFoundException(e.status_code, e.reason)
    if isinstance(e, (NotAuthorizedException, ForbiddenException)):
        return PermissionException(e.status_code, e.reason)
    return e
