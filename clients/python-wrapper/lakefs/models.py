"""
Module containing all of lakeFS data models
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Literal

from lakefs.namedtuple import LenientNamedTuple


class Commit(LenientNamedTuple):
    """
    NamedTuple representing a lakeFS commit's properties
    """
    id: str
    parents: List[str]
    committer: str
    message: str
    creation_date: int
    meta_range_id: str
    metadata: Optional[dict[str, str]] = None


class Change(LenientNamedTuple):
    """
    NamedTuple representing a diff change between two refs in lakeFS
    """
    type: Literal["added", "removed", "changed", "conflict", "prefix_changed"]
    path: str
    path_type: Literal["common_prefix", "object"]
    size_bytes: Optional[int]


class ImportStatus(LenientNamedTuple):
    """
    NamedTuple representing an ongoing import's status in lakeFS
    """

    class _Error(LenientNamedTuple):
        message: str

    completed: bool
    update_time: datetime
    ingested_objects: Optional[int]
    metarange_id: Optional[str]
    commit: Optional[Commit]
    error: Optional[_Error]

    def __init__(self, **kwargs):
        commit = kwargs.get("commit")
        if commit is not None:
            kwargs["commit"] = Commit(**commit)

        error = kwargs.get("error")
        if error is not None:
            kwargs["error"] = ImportStatus._Error(**error)

        super().__init__(**kwargs)


class ServerStorageConfiguration(LenientNamedTuple):
    """
    Represent a lakeFS server's storage configuration
    """
    blockstore_type: str
    pre_sign_support: bool
    import_support: bool
    blockstore_namespace_example: str
    blockstore_namespace_validity_regex: str
    pre_sign_support_ui: bool
    import_validity_regex: str
    default_namespace_prefix: Optional[str] = None


class ObjectStats(LenientNamedTuple):
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


class RepositoryProperties(LenientNamedTuple):
    """
    Represent a lakeFS repository's properties
    """
    id: str
    creation_date: int
    default_branch: str
    storage_namespace: str
