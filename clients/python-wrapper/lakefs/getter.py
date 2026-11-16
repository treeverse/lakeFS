"""
Module containing str-or-Object getters

This can help break import cycles.  To convert Repository | str to
Repository in another client module you can

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from lakefs.repository import Repository
from lakefs.getter import repository_for

repo: Repository = repository_for('abc', client)
"""

from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from lakefs.client import Client
if TYPE_CHECKING:
    from lakefs.repository import Repository


def repository_for(repo: Repository | str, client: Optional[Client]) -> Repository:
    """Return repo, or a Repository for it if it is a repository_id"""
    # Import here to break circular import.  Do not import the module: after
    # lakefs is loaded, lakefs.repository is the lakefs.repository() function.
    from lakefs.repository import Repository  # pylint: disable=import-outside-toplevel,redefined-outer-name
    return Repository(repo, client) if isinstance(repo, str) else repo

def repository_id(repo: Repository | str) -> str:
    """Return the repository_id of repo, or itself if it is already a repository_id"""
    return repo if isinstance(repo, str) else repo.id
