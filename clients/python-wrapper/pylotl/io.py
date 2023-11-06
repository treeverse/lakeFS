from io import BytesIO, TextIOWrapper
from typing import Optional, Literal, TextIO

from lakefs_sdk.exceptions import NotFoundException
from pylotl.client import Client
from pylotl.exceptions import ObjectExistsException

_range_str = "bytes={start}-{end}"


class ReadableObject:
    _client: Client
    _repo: str
    _ref: str
    _path: str
    _pos: int
    _pre_sign: bool

    def __init__(self, client: Client, repository: str, reference: str, path: str, pre_sign: bool = None) -> None:
        self._client = client
        self._repo = repository
        self._ref = reference
        self._path = path
        self._pre_sign = pre_sign or client.storage_config.pre_sign_support
        self._pos = 0

    @property
    def pos(self):
        return self._pos

    def seek(self, pos):
        if pos < 0:
            raise ValueError("position must be a non-negative integer")
        self._pos = pos

    def read(self, read_bytes: int = None) -> bytes:
        stat = self._client.sdk_client.objects_api.stat_object(self._repo, self._ref, self._path)
        read_bytes = read_bytes or stat.size_bytes
        new_pos = min(self._pos + read_bytes, stat.size_bytes) - 1
        read_range = _range_str.format(start=self._pos, end=new_pos)
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
    def __init__(self, **kwargs) -> None:
        # Verify that reference is a branch, otherwise throws exception
        client = kwargs["client"]
        client.sdk_client.branches_api.get_branch(kwargs["repository"], kwargs["reference"])
        super().__init__(**kwargs)

    # TODO: support streams
    def create(self,
               data: bytes | str | TextIO | BytesIO | TextIOWrapper,
               mode: Literal['x', 'xb', 'w', 'wb'] = 'wb',
               pre_sign: Optional[bool] = None,
               content_type: Optional[str] = None,
               metadata: Optional[dict[str, str]] = None):
        if mode.startswith('x') and self.exists():  # Requires explicit create
            raise ObjectExistsException
        # TODO: what to do with 'wb' vs 'w'?
        bytes_io = BytesIO(data.encode())
        is_presign = pre_sign or self._pre_sign
        return self._client.upload(self._repo, self._ref, self._path, data, is_presign, content_type, metadata)
