"""
Module containing lakeFS reference implementation
"""

from __future__ import annotations

import base64
import binascii
import io
import json
import os
import tempfile
import urllib.parse
from abc import abstractmethod
from typing import AnyStr, IO, Iterator, List, Literal, Optional, Union, get_args

import lakefs_sdk
import urllib3
from lakefs_sdk import StagingMetadata

from lakefs.client import Client, _BaseLakeFSObject
from lakefs.exceptions import (
    api_exception_handler,
    handle_http_error,
    LakeFSException,
    NotFoundException,
    ObjectNotFoundException,
    NotAuthorizedException,
    ForbiddenException,
    PermissionException,
    ObjectExistsException,
    InvalidRangeException,
)
from lakefs.models import ObjectInfo

_LAKEFS_METADATA_PREFIX = "x-lakefs-meta-"
# _BUFFER_SIZE - Writer buffer size. While buffer size not exceed, data will be maintained in memory and file will
#                not be created.
_WRITER_BUFFER_SIZE = 32 * 1024 * 1024

ReadModes = Literal['r', 'rb']
WriteModes = Literal['x', 'xb', 'w', 'wb']


class LakeFSIOBase(_BaseLakeFSObject, IO):
    """
    Base class for the lakeFS Reader and Writer classes
    """
    _obj: StoredObject
    _mode: ReadModes
    _pos: int
    _pre_sign: Optional[bool] = None
    _is_closed: bool = False

    def __init__(self, obj: StoredObject, mode: Union[ReadModes, WriteModes], pre_sign: Optional[bool] = None,
                 client: Optional[Client] = None) -> None:
        self._obj = obj
        self._mode = mode
        self._pre_sign = pre_sign if pre_sign is not None else client.storage_config.pre_sign_support
        self._pos = 0
        super().__init__(client)

    @property
    def mode(self) -> str:
        """
        Returns the open mode for this object
        """
        return self._mode

    @property
    def name(self) -> str:
        """
        Returns the name of the object relative to the repo and reference
        """
        return self._obj.path

    @property
    def closed(self) -> bool:
        """
        Returns True after the object is closed
        """
        return self._is_closed

    def close(self) -> None:
        """
        Closes the current file descriptor for IO operations
        """
        self._is_closed = True

    def fileno(self) -> int:
        """
        The file descriptor number as defined by the operating system. In the context of lakeFS it has no meaning

        :return: -1 Always
        """
        return -1

    @abstractmethod
    def flush(self) -> None:
        raise NotImplementedError

    def isatty(self) -> bool:
        """
        Irrelevant for the lakeFS implementation
        """
        return False

    @abstractmethod
    def readable(self) -> bool:
        raise NotImplementedError

    def readline(self, limit: int = -1):
        """
        Must be explicitly implemented by inheriting class
        """
        raise io.UnsupportedOperation

    def readlines(self, hint: int = -1):
        """
        Must be explicitly implemented by inheriting class
        """
        raise io.UnsupportedOperation

    @abstractmethod
    def seekable(self) -> bool:
        raise NotImplementedError

    def truncate(self, size: int = None) -> int:
        """
        Unsupported by lakeFS implementation
        """
        raise io.UnsupportedOperation

    @abstractmethod
    def writable(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def write(self, s: AnyStr) -> int:
        raise NotImplementedError

    def writelines(self, lines: List[AnyStr]) -> None:
        """
        Unsupported by lakeFS implementation
        """
        raise io.UnsupportedOperation

    def __next__(self) -> AnyStr:
        line = self.readline()
        if len(line) == 0:
            raise StopIteration
        return line

    def __iter__(self) -> Iterator[AnyStr]:
        return self

    def __enter__(self) -> LakeFSIOBase:
        return self

    def __exit__(self, typ, value, traceback) -> bool:
        self.close()
        return False  # Don't suppress an exception

    @abstractmethod
    def seek(self, offset: int, whence: int = 0) -> int:
        raise NotImplementedError

    @abstractmethod
    def read(self, n: int = None) -> str | bytes:
        raise NotImplementedError

    def tell(self) -> int:
        """
        For readers - read position, for writers can be used as an indication for bytes written
        """
        return self._pos


class ObjectReader(LakeFSIOBase):
    """
    ObjectReader provides read-only functionality for lakeFS objects with IO semantics.
    This Object is instantiated and returned for immutable reference types (Commit, Tag...)
    """

    _readlines_buf: io.BytesIO

    def __init__(self, obj: StoredObject, mode: ReadModes, pre_sign: Optional[bool] = None,
                 client: Optional[Client] = None) -> None:
        if mode not in get_args(ReadModes):
            raise ValueError(f"invalid read mode: '{mode}'. ReadModes: {ReadModes}")

        self._readlines_buf = io.BytesIO(b"")

        super().__init__(obj, mode, pre_sign, client)

    @property
    def pre_sign(self):
        """
        Returns whether the pre_sign mode is enabled
        """
        if self._pre_sign is None:
            self._pre_sign = self._client.storage_config.pre_sign_support
        return self._pre_sign

    def readable(self) -> bool:
        """
        Returns True always
        """
        return True

    def write(self, s: AnyStr) -> int:
        """
        Unsupported for reader object
        """
        raise io.UnsupportedOperation

    def seekable(self) -> bool:
        """
        Returns True always
        """
        return True

    def writable(self) -> bool:
        """
        Unsupported - read only object
        """
        return False

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Move the object's reading position

        :param offset: The offset from the beginning of the file
        :param whence: Optional. The whence argument is optional and defaults to
            os.SEEK_SET or 0 (absolute file positioning);
            other values are os.SEEK_CUR or 1 (seek relative to the current position) and os.SEEK_END or 2
            (seek relative to the file’s end)
        :raise OSError: if calculated new position is negative
        :raise io.UnsupportedOperation: If whence value is unsupported
        """
        if whence == os.SEEK_SET:
            pos = offset
        elif whence == os.SEEK_CUR:
            pos = self._pos + offset
        elif whence == os.SEEK_END:
            size = self._obj.stat().size_bytes  # Seek end requires us to know the size of the file
            pos = size + offset
        else:
            raise io.UnsupportedOperation(f"whence={whence} is not supported")

        if pos < 0:
            raise OSError("position must be a non-negative integer")
        self._pos = pos
        return pos

    def _cast_by_mode(self, retval):
        if 'b' not in self.mode:
            return retval.decode('utf-8')
        return retval

    def _read(self, read_range: str) -> str | bytes:
        try:
            with api_exception_handler(_io_exception_handler):
                return self._client.sdk_client.objects_api.get_object(self._obj.repo,
                                                                      self._obj.ref,
                                                                      self._obj.path,
                                                                      range=read_range,
                                                                      presign=self.pre_sign)

        except InvalidRangeException:
            # This is done in order to behave like the built-in open() function
            return b''

    def read(self, n: int = None) -> str | bytes:
        """
        Read object data

        :param n: How many bytes to read. If read_bytes is None, will read from current position to end.
            If current position + read_bytes > object size.
        :return: The bytes read
        :raise OSError: if read_bytes is non-positive
        :raise ObjectNotFoundException: if repository id, reference id or object path does not exist
        :raise PermissionException: if user is not authorized to perform this operation, or operation is forbidden
        :raise ServerException: for any other errors
        """
        if n and n <= 0:
            raise OSError("read_bytes must be a positive integer")

        read_range = self._get_range_string(start=self._pos, read_bytes=n)
        contents = self._read(read_range)
        self._pos += len(contents)  # Update pointer position

        return self._cast_by_mode(contents)

    def readline(self, limit: int = -1):
        """
        Read and return a line from the stream.

        :param limit: If limit > -1 returns at most limit bytes
        """
        if self._readlines_buf.getbuffer().nbytes == 0:
            self._readlines_buf = io.BytesIO(self._read(self._get_range_string(0)))
        self._readlines_buf.seek(self._pos)
        line = self._readlines_buf.readline(limit)
        self._pos = self._readlines_buf.tell()
        return self._cast_by_mode(line)

    def flush(self) -> None:
        """
        Nothing to do for reader
        """

    @staticmethod
    def _get_range_string(start, read_bytes=None):
        if start == 0 and read_bytes is None:
            return None
        if read_bytes is None:
            return f"bytes={start}-"
        return f"bytes={start}-{start + read_bytes - 1}"

    def __str__(self):
        return self._obj.path

    def __repr__(self):
        return f'ObjectReader(path="{self._obj.path}")'


class ObjectWriter(LakeFSIOBase):
    """
    ObjectWriter provides write-only functionality for lakeFS objects with IO semantics.
    This Object is instantiated and returned from the WriteableObject writer method.
    For the data to be actually written to the lakeFS server the close() method must be invoked explicitly or
    implicitly when using writer as a context.
    """
    _fd: tempfile.SpooledTemporaryFile
    _obj_stats: ObjectInfo = None

    def __init__(self,
                 obj: StoredObject,
                 mode: WriteModes,
                 pre_sign: Optional[bool] = None,
                 content_type: Optional[str] = None,
                 metadata: Optional[dict[str, str]] = None,
                 client: Optional[Client] = None) -> None:

        if 'x' in mode and obj.exists():  # Requires explicit create
            raise ObjectExistsException

        if mode not in get_args(WriteModes):
            raise ValueError(f"invalid write mode: '{mode}'. WriteModes: {WriteModes}")

        self.content_type = content_type
        self.metadata = metadata

        open_kwargs = {
            "encoding": "utf-8" if 'b' not in mode else None,
            "mode": 'wb+' if 'b' in mode else 'w+',
            "max_size": _WRITER_BUFFER_SIZE,
        }
        self._fd = tempfile.SpooledTemporaryFile(**open_kwargs)  # pylint: disable=consider-using-with
        super().__init__(obj, mode, pre_sign, client)

    @property
    def pre_sign(self) -> bool:
        """
        Returns whether the pre_sign mode is enabled
        """
        if self._pre_sign is None:
            self._pre_sign = self._client.storage_config.pre_sign_support
        return self._pre_sign

    @pre_sign.setter
    def pre_sign(self, value: bool) -> None:
        """
        Set the pre_sign mode to value

        :param value: The new value for pre_sign mode
        """
        self._pre_sign = value

    def flush(self) -> None:
        """
        Flush buffer to file. Prevent flush if total write size is still smaller than _BUFFER_SIZE so that we avoid
        unnecessary write to disk.
        """
        # Don't flush buffer to file if we didn't exceed buffer size
        # We want to avoid using the file if possible
        if self._pos > _WRITER_BUFFER_SIZE:
            self._fd.flush()

    def write(self, s: AnyStr) -> int:
        """
        Write data to buffer

        :param s: The data to write
        :return: The number of bytes written to buffer
        """
        binary_mode = 'b' in self._mode
        if binary_mode and isinstance(s, str):
            contents = s.encode('utf-8')
        elif not binary_mode and isinstance(s, bytes):
            contents = s.decode('utf-8')
        else:
            contents = s

        count = self._fd.write(contents)
        self._pos += count
        return count

    def close(self) -> None:
        """
        Write the data to the lakeFS server and close open descriptors
        """
        stats = self._upload_presign() if self.pre_sign else self._upload_raw()
        self._obj_stats = ObjectInfo(**stats.dict())

        self._fd.close()
        super().close()

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

    def _upload_raw(self) -> lakefs_sdk.ObjectStats:
        """
        Use raw upload API call to bypass validation of content parameter
        """
        auth_settings = ['basic_auth', 'cookie_auth', 'oidc_auth', 'saml_auth', 'jwt_token']
        headers = {
            "Accept": "application/json",
            "Content-Type": self.content_type if self.content_type is not None else "application/octet-stream"
        }

        # Create user metadata headers
        if self.metadata is not None:
            for k, v in self.metadata.items():
                headers[_LAKEFS_METADATA_PREFIX + k] = v

        self._fd.seek(0)
        resource_path = urllib.parse.quote(f"/repositories/{self._obj.repo}/branches/{self._obj.ref}/objects",
                                           encoding="utf-8")
        query_params = urllib.parse.urlencode({"path": self._obj.path}, encoding="utf-8")
        url = self._client.config.host + resource_path + f"?{query_params}"
        self._client.sdk_client.objects_api.api_client.update_params_for_auth(headers, None, auth_settings,
                                                                              resource_path, "POST", self._fd)
        resp = self._client.sdk_client.objects_api.api_client.rest_client.pool_manager.request(url=url,
                                                                                               method="POST",
                                                                                               headers=headers,
                                                                                               body=self._fd)

        handle_http_error(resp)
        return lakefs_sdk.ObjectStats(**json.loads(resp.data))

    def _upload_presign(self) -> lakefs_sdk.ObjectStats:
        staging_location = self._client.sdk_client.staging_api.get_physical_address(self._obj.repo,
                                                                                    self._obj.ref,
                                                                                    self._obj.path,
                                                                                    True)
        url = staging_location.presigned_url

        headers = {"Content-Length": self._pos}
        if self.content_type:
            headers["Content-Type"] = self.content_type
        if self._client.storage_config.blockstore_type == "azure":
            headers["x-ms-blob-type"] = "BlockBlob"

        self._fd.seek(0)
        resp = urllib3.request(method="PUT",
                               url=url,
                               body=self._fd,
                               headers=headers)
        handle_http_error(resp)

        etag = ObjectWriter._extract_etag_from_response(resp.getheaders())
        size_bytes = self._pos
        staging_metadata = StagingMetadata(staging=staging_location,
                                           size_bytes=size_bytes,
                                           checksum=etag,
                                           user_metadata=self.metadata,
                                           content_type=self.content_type)
        return self._client.sdk_client.staging_api.link_physical_address(self._obj.repo,
                                                                         self._obj.ref,
                                                                         self._obj.path,
                                                                         staging_metadata=staging_metadata)

    def readable(self) -> bool:
        """
        ObjectWriter is write-only - return False always
        """
        return False

    def seekable(self) -> bool:
        """
        ObjectWriter is not seekable. Returns False always
        """
        return False

    def writable(self) -> bool:
        """
        Returns True always
        """
        return True

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Unsupported for writer class
        """
        raise io.UnsupportedOperation

    def read(self, n: int = None) -> str | bytes:
        """
        Unsupported for writer class
        """
        raise io.UnsupportedOperation

    def __repr__(self):
        return f'ObjectWriter(path="{self._obj.path}")'


class StoredObject(_BaseLakeFSObject):
    """
    Class representing an object in lakeFS.
    """
    _repo_id: str
    _ref_id: str
    _path: str
    _stats: Optional[ObjectInfo] = None

    def __init__(self, repository: str, reference: str, path: str, client: Optional[Client] = None):
        self._repo_id = repository
        self._ref_id = reference
        self._path = path
        super().__init__(client)

    def __str__(self) -> str:
        return self.path

    def __repr__(self):
        return f'StoredObject(repository="{self.repo}", reference="{self.ref}", path="{self.path}")'

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

    def reader(self, mode: ReadModes = 'rb', pre_sign: Optional[bool] = None) -> ObjectReader:
        """
        Context manager which provide a file-descriptor like object that allow reading the given object.

        Usage Example:

        .. code-block:: python

            import lakefs

            obj = lakefs.repository("<repository_name>").branch("<branch_name>").object("file.txt")
            file_size = obj.stat().size_bytes

            with obj.reader(mode='r', pre_sign=True) as fd:
                # print every other 10 chars
                while fd.tell() < file_size
                    print(fd.read(10))
                    fd.seek(10, os.SEEK_CUR)

        :param mode: Read mode - as supported by ReadModes
        :param pre_sign: (Optional), enforce the pre_sign mode on the lakeFS server. If not set, will probe server for
            information.
        :return: A Reader object
        """
        return ObjectReader(self, mode=mode, pre_sign=pre_sign, client=self._client)

    def stat(self) -> ObjectInfo:
        """
        Return the Stat object representing this object
        """
        if self._stats is None:
            with api_exception_handler(_io_exception_handler):
                stat = self._client.sdk_client.objects_api.stat_object(self._repo_id, self._ref_id, self._path)
                self._stats = ObjectInfo(**stat.dict())
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
        :raise ObjectNotFoundException: if repo id,reference id, destination branch id or object path does not exist
        :raise PermissionException: if user is not authorized to perform this operation, or operation is forbidden
        :raise ServerException: for any other errors
        """

        with api_exception_handler():
            object_copy_creation = lakefs_sdk.ObjectCopyCreation(src_ref=self._ref_id, src_path=self._path)
            self._client.sdk_client.objects_api.copy_object(repository=self._repo_id,
                                                            branch=destination_branch_id,
                                                            dest_path=destination_path,
                                                            object_copy_creation=object_copy_creation)

        return WriteableObject(repository=self._repo_id, reference=destination_branch_id, path=destination_path,
                               client=self._client)


class WriteableObject(StoredObject):
    """
    WriteableObject inherits from ReadableObject and provides read/write functionality for lakeFS objects
    using IO semantics.
    This Object is instantiated and returned upon invoking writer() on Branch reference type.
    """

    def __init__(self, repository: str, reference: str, path: str,
                 client: Optional[Client] = None) -> None:
        super().__init__(repository, reference, path, client=client)

    def __repr__(self):
        return f'WriteableObject(repository="{self.repo}", reference="{self.ref}", path="{self.path}")'

    def upload(self,
               data: str | bytes,
               mode: WriteModes = 'w',
               pre_sign: Optional[bool] = None,
               content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None) -> WriteableObject:
        """
        Upload a new object or overwrites an existing object

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
        :raise ObjectExistsException: if object exists and mode is exclusive ('x')
        :raise ObjectNotFoundException: if repo id, reference id or object path does not exist
        :raise PermissionException: if user is not authorized to perform this operation, or operation is forbidden
        :raise ServerException: for any other errors
        """
        with ObjectWriter(self, mode, pre_sign, content_type, metadata, self._client) as writer:
            writer.write(data)

        return self

    def delete(self) -> None:
        """
        Delete object from lakeFS

        :raise ObjectNotFoundException: if repo id, reference id or object path does not exist
        :raise PermissionException: if user is not authorized to perform this operation, or operation is forbidden
        :raise ServerException: for any other errors
        """
        with api_exception_handler(_io_exception_handler):
            self._client.sdk_client.objects_api.delete_object(self._repo_id, self._ref_id, self._path)
            self._stats = None

    def writer(self,
               mode: WriteModes = 'wb',
               pre_sign: Optional[bool] = None,
               content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None) -> ObjectWriter:
        """
        Context manager which provide a file-descriptor like object that allow writing the given object to lakeFS
        The writes are saved in a buffer as long as the writer is open. Only when it closes it writes the data into
        lakeFS. The optional parameters can be modified by accessing the respective fields as long as the writer is
        still open.

        Usage example of reading a file from local file system and writing it to lakeFS:

        .. code-block:: python

            import lakefs

            obj = lakefs.repository("<repository_name>").branch("<branch_name>").object("my_image")

            with open("my_local_image", mode='rb') as reader, obj.writer("wb") as writer:
                writer.write(reader.read())

        :param mode: Write mode - as supported by WriteModes
        :param pre_sign: (Optional), enforce the pre_sign mode on the lakeFS server. If not set, will probe server for
            information.
        :param content_type: (Optional) Specify the data media type
        :param metadata: (Optional) User defined metadata to save on the object
        :return: A Writer object
        """
        return ObjectWriter(self,
                            mode=mode,
                            pre_sign=pre_sign,
                            content_type=content_type,
                            metadata=metadata,
                            client=self._client)


def _io_exception_handler(e: LakeFSException):
    if isinstance(e, NotFoundException):
        return ObjectNotFoundException(e.status_code, e.reason)
    if isinstance(e, (NotAuthorizedException, ForbiddenException)):
        return PermissionException(e.status_code, e.reason)
    return e
