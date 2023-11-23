"""
Module containing lakeFS reference implementation
"""

from __future__ import annotations

import base64
import binascii
import codecs
from contextlib import contextmanager
from typing import Optional, Literal, NamedTuple, Union, Iterable, AsyncIterable, get_args
from io import IOBase, BytesIO, StringIO, TextIOWrapper, BufferedReader, BufferedRandom
import lakefs_sdk
from lakefs_sdk import StagingMetadata

from lakefs.client import Client, DefaultClient
from lakefs.exceptions import (
    api_exception_handler,
    LakeFSException,
    NotFoundException,
    ObjectNotFoundException,
    NotAuthorizedException,
    ForbiddenException,
    PermissionException,
    ObjectExistsException
)

_LAKEFS_METADATA_PREFIX = "x-lakefs-meta-"

# Type to support both strings and bytes in addition to streams
UploadContentType = Union[str, bytes, IOBase]
OpenModes = Literal['r', 'rb']
WriteModes = Literal['x', 'xb', 'w', 'wb']


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


class StoredObject:
    """
    Class representing an object in lakeFS.
    """
    _client: Client
    _repo_id: str
    _ref_id: str
    _path: str
    _stats: Optional[ObjectStats] = None

    def __init__(self, repository: str, reference: str, path: str, client: Optional[Client] = DefaultClient):
        self._client = client
        self._repo_id = repository
        self._ref_id = reference
        self._path = path

    @property
    def repo(self) -> str:
        """
        Returns the object's repository id
        """
        return self._repo_id

    @property
    def ref(self) -> str:
        """
        Returns the object's reference id
        """
        return self._ref_id

    @property
    def path(self) -> str:
        """
        Returns the object's path relative to repository and reference ids
        """
        return self._path

    @contextmanager
    def open(self, mode: OpenModes = 'r', pre_sign: Optional[bool] = None) -> ObjectReader:
        """
        Context manager which provide a file-descriptor like object that allow reading the given object
        :param mode: Open mode - as supported by OpenModes
        :param pre_sign: (Optional), enforce the pre_sign mode on the lakeFS server. If not set, will probe server for
        information.
        :return: A Reader object
        """
        reader = ObjectReader(self, mode=mode, pre_sign=pre_sign, client=self._client)
        yield reader

    def stat(self) -> ObjectStats:
        """
        Return the Stat object representing this object
        """
        if self._stats is None:
            with api_exception_handler(_io_exception_handler):
                stat = self._client.sdk_client.objects_api.stat_object(self._repo_id, self._ref_id, self._path)
                self._stats = ObjectStats(**stat.__dict__)
        return self._stats

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
            self._client.sdk_client.objects_api.head_object(self._repo_id, self._ref_id, self._path)
            exists = True

        return exists

    def copy(self, destination_branch_id: str, destination_path: str) -> WriteableObject:
        """
        Copy the object to a destination branch
        :param destination_branch_id: The destination branch to copy the object to
        :param destination_path: The path of the copied object in the destination branch
        :return: The newly copied Object
        :raises:
            ObjectNotFoundException if repo id,reference id, destination branch id or object path does not exist
            PermissionException if user is not authorized to perform this operation, or operation is forbidden
            ServerException for any other errors
        """

        with api_exception_handler():
            object_copy_creation = lakefs_sdk.ObjectCopyCreation(src_ref=self._ref_id, src_path=self._path)
            self._client.sdk_client.objects_api.copy_object(repository=self._repo_id,
                                                            branch=destination_branch_id,
                                                            dest_path=destination_path,
                                                            object_copy_creation=object_copy_creation)

        return WriteableObject(repository=self._repo_id, reference=destination_branch_id, path=destination_path,
                               client=self._client)


class ObjectReader:
    """
    ReadableObject provides read-only functionality for lakeFS objects with IO semantics.
    This Object is instantiated and returned on open() methods for immutable reference types (Commit, Tag...)
    """
    _client: Client
    _obj: StoredObject
    _mode: OpenModes
    _pos: int
    _pre_sign: Optional[bool] = None

    def __init__(self, obj: StoredObject, mode: OpenModes, pre_sign: Optional[bool] = None,
                 client: Optional[Client] = DefaultClient) -> None:
        if mode not in get_args(OpenModes):
            raise ValueError(f"invalid read mode: '{mode}'. ReadModes: {OpenModes}")

        self._obj = obj
        self._mode = mode
        self._pre_sign = pre_sign if pre_sign is not None else client.storage_config.pre_sign_support
        self._client = client
        self._pos = 0

    def tell(self) -> int:
        """
        Object's read position. If position is past object byte size, trying to read the object will return EOF
        """
        return self._pos

    def seek(self, pos) -> None:
        """
        Move the object's reading position
        :param pos: The position (in bytes) to move to
        :raises OSError if provided position is negative
        """
        if pos < 0:
            raise OSError("position must be a non-negative integer")
        self._pos = pos

    @staticmethod
    def _get_range_string(start, read_bytes=None):
        if start == 0 and read_bytes is None:
            return None
        if read_bytes is None:
            return f"bytes={start}-"
        return f"bytes={start}-{start + read_bytes - 1}"

    def read(self, read_bytes: int = None) -> str | bytes:
        """
        Read object data
        :param read_bytes: How many bytes to read. If read_bytes is None, will read from current position to end.
        If current position + read_bytes > object size.
        :return: The bytes read
        :raises
            EOFError if current position is after object size
            OSError if read_bytes is non-positive
            ObjectNotFoundException if repo id, reference id or object path does not exist
            PermissionException if user is not authorized to perform this operation, or operation is forbidden
            ServerException for any other errors
        """
        if read_bytes and read_bytes <= 0:
            raise OSError("read_bytes must be a positive integer")

        read_range = self._get_range_string(start=self._pos, read_bytes=read_bytes)
        with api_exception_handler(_io_exception_handler):
            contents = self._client.sdk_client.objects_api.get_object(self._obj.repo,
                                                                      self._obj.ref,
                                                                      self._obj.path,
                                                                      range=read_range,
                                                                      presign=self._pre_sign)
        self._pos += len(contents)  # Update pointer position
        if 'b' not in self._mode:
            return contents.decode('utf-8')

        return contents


class WriteableObject(StoredObject):
    """
    WriteableObject inherits from ReadableObject and provides read/write functionality for lakeFS objects
    using IO semantics.
    This Object is instantiated and returned upon invoking open() on Branch reference type.
    """

    def __init__(self, repository: str, reference: str, path: str, client: Optional[Client] = DefaultClient) -> None:
        super().__init__(repository, reference, path, client=client)

    def create(self,
               data: UploadContentType,
               mode: WriteModes = 'wb',
               pre_sign: Optional[bool] = None,
               content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None) -> WriteableObject:
        """
        Creates a new object or overwrites an existing object
        :param data: The contents of the object to write (can be bytes or string)
        :param mode: Write mode:
            'x'     - Open for exclusive creation
            'xb'    - Open for exclusive creation in binary mode
            'w'     - Create a new object or truncate if exists
            'wb'    - Create or truncate in binary mode
        :param pre_sign: (Optional) Explicitly state whether to use pre_sign mode when uploading the object.
        If None, will be taken from pre_sign property.
        :param content_type: (Optional) Explicitly set the object Content-Type
        :param metadata: (Optional) User metadata
        :return: The Stat object representing the newly created object
        :raises
            ObjectExistsException if object exists and mode is exclusive ('x')
            ObjectNotFoundException if repo id, reference id or object path does not exist
            PermissionException if user is not authorized to perform this operation, or operation is forbidden
            ServerException for any other errors
        """
        if mode not in get_args(WriteModes):
            raise ValueError(f"invalid write mode: '{mode}'. WriteModes: {WriteModes}")

        if 'x' in mode and self.exists():  # Requires explicit create
            raise ObjectExistsException

        contents = WriteableObject._get_contents_from_data(data, 'b' in mode)

        is_presign = pre_sign if pre_sign is not None else self._client.storage_config.pre_sign_support
        with api_exception_handler(_io_exception_handler):
            stats = self._upload_presign(contents, content_type, metadata) if is_presign \
                else self._upload_raw(contents, content_type, metadata)
            self._stats = ObjectStats(**stats.__dict__)

        return self

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

    @staticmethod
    def _get_size_bytes(contents: UploadContentType) -> int:
        if isinstance(contents, str):
            return len(contents.encode('utf-8'))
        elif isinstance(contents, IOBase):  # Type streams
            return contents.tell()
        else:  # Type Iterable / bytes
            return len(contents)

    @staticmethod
    def _get_contents_from_data(data: UploadContentType, binary_mode: bool) -> IOBase:
        # First transform data into stream
        if isinstance(data, str):
            contents = BytesIO(data.encode('utf-8'))
        elif isinstance(data, bytes):
            contents = BytesIO(data)
        else:
            contents = data

        if binary_mode:
            # Wrap with a BinaryReader
            return BufferedReader(contents)
        else:
            return TextIOWrapper(contents, encoding='utf-8')

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
        if metadata is not None:
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
            path_params={"repository": self._repo_id, "branch": self._ref_id},
            query_params={"path": self._path},
            header_params=headers,
            body=content,
            response_types_map=_response_types_map,
            auth_settings=_auth_settings,
            _return_http_data_only=True
        )

    def _upload_presign(self, contents: IOBase, content_type: Optional[str] = None,
                        metadata: Optional[dict[str, str]] = None) -> ObjectStats:
        headers = {
            "Accept": "application/json",
            "Content-Type": content_type if content_type is not None else "application/octet-stream"
        }

        staging_location = self._client.sdk_client.staging_api.get_physical_address(self._repo_id,
                                                                                    self._ref_id,
                                                                                    self._path,
                                                                                    True)
        url = staging_location.presigned_url
        if self._client.storage_config.blockstore_type == "azure":
            headers["x-ms-blob-type"] = "BlockBlob"

        resp = self._client.sdk_client.objects_api.api_client.request(
            method="PUT",
            url=url,
            body=contents,
            headers=headers
        )

        etag = WriteableObject._extract_etag_from_response(resp.getheaders())

        staging_metadata = StagingMetadata(staging=staging_location,
                                           size_bytes=contents.tell(),
                                           checksum=etag,
                                           user_metadata=metadata)
        return self._client.sdk_client.staging_api.link_physical_address(self._repo_id, self._ref_id, self._path,
                                                                         staging_metadata=staging_metadata)

    def delete(self) -> None:
        """
        Delete object from lakeFS
            ObjectNotFoundException if repo id, reference id or object path does not exist
            PermissionException if user is not authorized to perform this operation, or operation is forbidden
            ServerException for any other errors
        """
        with api_exception_handler(_io_exception_handler):
            self._client.sdk_client.objects_api.delete_object(self._repo_id, self._ref_id, self._path)
            self._stats = None


def _io_exception_handler(e: LakeFSException):
    if isinstance(e, NotFoundException):
        return ObjectNotFoundException(e.status_code, e.reason)
    if isinstance(e, (NotAuthorizedException, ForbiddenException)):
        return PermissionException(e.status_code, e.reason)
    return e
