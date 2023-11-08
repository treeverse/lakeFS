from typing import Optional, Literal, Union, Iterable, AsyncIterable

from lakefs_sdk.exceptions import NotFoundException
from lakefs.client import Client
from lakefs.exceptions import ObjectExistsException, UnsupportedOperationException, ObjectNotFoundException

_RANGE_STR_TMPL = "bytes={start}-{end}"

# Type to support both strings and bytes in addition to streams (reference: httpx._types.RequestContent)
UploadContentType = Union[str, bytes, Iterable[bytes], AsyncIterable[bytes]]


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
    _pre_sign: bool

    def __init__(self, repository: str, reference: str, path: str,
                 pre_sign: Optional[bool] = None, client: Optional[Client] = None) -> None:
        self._client = client
        self._repo = repository
        self._ref = reference
        self._path = path
        self._pre_sign = pre_sign if pre_sign is not None else client.storage_config.pre_sign_support
        self._pos = 0

    @property
    def pos(self):
        return self._pos

    def seek(self, pos):
        if pos < 0:
            raise ValueError("position must be a non-negative integer")
        self._pos = pos

    def read(self, read_bytes: int = None) -> bytes:
        if read_bytes and read_bytes <= 0:
            raise ValueError("read_bytes must be a positive integer")
        try:
            stat = self._client.sdk_client.objects_api.stat_object(self._repo, self._ref, self._path)
        except NotFoundException:
            raise ObjectNotFoundException
        if self._pos >= stat.size_bytes:
            raise EOFError
        read_bytes = read_bytes if read_bytes is not None else stat.size_bytes
        new_pos = min(self._pos + read_bytes, stat.size_bytes)
        read_range = _RANGE_STR_TMPL.format(start=self._pos, end=new_pos - 1)
        contents = self._client.sdk_client.objects_api.get_object(self._repo,
                                                                  self._ref,
                                                                  self._path,
                                                                  range=read_range,
                                                                  presign=self._pre_sign)
        self._pos = new_pos  # Update pointer position
        return contents

    def stat(self):
        return self._client.sdk_client.objects_api.stat_object(self._repo, self._ref, self._path)

    def exists(self):
        try:
            self._client.sdk_client.objects_api.head_object(self._repo, self._ref, self._path)
            return True
        except NotFoundException:
            return False


class WriteableObject(ReadableObject):
    """
    WriteableObject inherits from ReadableObject and provides read/write functionality for lakeFS objects 
    using IO semantics.
    This Object is instantiated and returned upon invoking open() on Branch reference type.
    """

    def __init__(self, repository: str, reference: str, path: str,
                 pre_sign: Optional[bool] = None, client: Optional[Client] = None) -> None:
        # Verify that reference is a branch, otherwise throws exception
        client = client
        try:
            client.sdk_client.branches_api.get_branch(repository, reference)
        except NotFoundException:
            raise UnsupportedOperationException("reference is not an existing branch")

        super().__init__(repository, reference, path, pre_sign, client=client)

    def create(self,
               data: UploadContentType,
               mode: Literal['x', 'xb', 'w', 'wb'] = 'wb',
               pre_sign: Optional[bool] = None,
               content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None):
        content = data
        if mode.startswith('x') and self.exists():  # Requires explicit create
            raise ObjectExistsException

        binary_mode = mode.endswith('b')
        if binary_mode and isinstance(data, str):
            content = data.encode('utf-8')
        elif not binary_mode and isinstance(data, bytes):
            content = data.decode('utf-8')
        # TODO: handle streams
        is_presign = pre_sign if pre_sign is not None else self._pre_sign
        stats = self._client.upload(self._repo, self._ref, self._path, content, is_presign, content_type, metadata)
        # reset pos after create
        self._pos = 0
        return stats

    def delete(self):
        # TODO: Implement
        raise NotImplementedError
