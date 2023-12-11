"""
Allow importing of models from package root
"""

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


def repository(repository_id: str) -> Repository:
    """
    Wrapper for getting a Repository object from the lakefs module.
    Enable more fluid syntax (lakefs.repository("x").branch("y") instead of lakefs.Repository("x").branch("y"))

    :param repository_id: The repository name
    :return: Repository object representing a lakeFS repository with the give repository_id
    """
    return Repository(repository_id)
