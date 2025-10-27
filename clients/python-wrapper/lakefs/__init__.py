"""
Allow importing of models from package root
"""
from lakefs.client import Client as Client
from lakefs.repository import (
    Repository as Repository,
    repositories as repositories
)
from lakefs.reference import Reference as Reference
from lakefs.models import (
    Commit as Commit,
    Change as Change,
    ImportStatus as ImportStatus,
    ServerStorageConfiguration as ServerStorageConfiguration,
    ObjectInfo as ObjectInfo,
    CommonPrefix as CommonPrefix,
    RepositoryProperties as RepositoryProperties
)
from lakefs.tag import Tag as Tag
from lakefs.branch import Branch as Branch
from lakefs.object import (
    StoredObject as StoredObject,
    WriteableObject as WriteableObject,
    ObjectReader as ObjectReader
)
from lakefs.branch import LakeFSDeprecationWarning as LakeFSDeprecationWarning


def repository(repository_id: str, *args, **kwargs) -> Repository:
    """
    Wrapper for getting a Repository object from the lakefs module.
    Enable more fluid syntax (lakefs.repository("x").branch("y") instead of lakefs.Repository("x").branch("y"))

    :param repository_id: The repository name
    :return: Repository object representing a lakeFS repository with the give repository_id
    """
    return Repository(repository_id, *args, **kwargs)
