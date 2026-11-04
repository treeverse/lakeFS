"""
Module containing str-or-Object getters

This can help break import cycles.  To convert Repository | str to
Repository in another client module you can

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from repository import Repository
from getter import repository_for

repo: Repository = repository_for('abc')
"""

from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from lakefs.client import Client
# Import all of repository to break circular import.
from lakefs import repository as repository_mod # Rename: repository is also a function.


def repository_for(repo: repository_mod.Repository | str, client: Optional[Client]) -> repository_mod.Repository:
    return repository_mod.Repository(repo, client) if isinstance(repo, str) else repo
