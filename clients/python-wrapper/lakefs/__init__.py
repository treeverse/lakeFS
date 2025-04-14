"""
Allow importing of models from package root
"""
from lakefs.client import Client
from lakefs.repository import Repository, repositories
from lakefs.reference import Reference
from lakefs.models import (
    Commit,
    Change,
    ImportStatus,
    ServerStorageConfiguration,
    ObjectInfo,
    CommonPrefix,
    RepositoryProperties
)
from lakefs.tag import Tag
from lakefs.branch import Branch
from lakefs.object import StoredObject, WriteableObject, ObjectReader
from lakefs.branch import LakeFSDeprecationWarning

from lakefs.cmd import find_or_download_binary, run_binary


def repository(repository_id: str, *args, **kwargs) -> Repository:
    """
    Wrapper for getting a Repository object from the lakefs module.
    Enable more fluid syntax (lakefs.repository("x").branch("y") instead of lakefs.Repository("x").branch("y"))

    :param repository_id: The repository name
    :return: Repository object representing a lakeFS repository with the give repository_id
    """
    return Repository(repository_id, *args, **kwargs)


def lakefs_cli() -> int:
    '''
    Wrapper around the lakefs cli that checks for the lakefs binary
    in $PATH or ~/.lakefs/bin/lakefs, downloads it if not found, and executes it.
    '''
    return run_binary(find_or_download_binary('lakefs'))


def lakectl_cli() -> int:
    '''
    Wrapper around the lakectl cli that checks for the lakectl binary
    in $PATH or ~/.lakefs/bin/lakectl, downloads it if not found, and executes it.
    '''
    return run_binary(find_or_download_binary('lakectl'))
