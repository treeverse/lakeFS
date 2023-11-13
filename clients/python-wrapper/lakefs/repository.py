"""
Module containing lakeFS repository implementation
"""
from __future__ import annotations
import http
from typing import Optional, NamedTuple

import lakefs_sdk

import lakefs.tag
import lakefs.branch
from lakefs.branch_manager import BranchManager
from lakefs.client import Client, DefaultClient
from lakefs.exceptions import RepositoryNotFoundException, NotAuthorizedException, ServerException
from lakefs.reference import Reference
from lakefs.tag_manager import TagManager


class RepositoryProperties(NamedTuple):
    id: str
    creation_date: int
    default_branch: str
    storage_namespace: str


class Repository:
    """
    Class representing a Repository in lakeFS.
    The Repository object provides the context for the other objects that are found in it.
    Access to these objects should be done from this class
    """
    _client: Client
    _id: str
    _properties: RepositoryProperties = None

    def __init__(self, repository_id: str, client: Optional[Client] = DefaultClient) -> None:
        self._id = repository_id
        self._client = client

    def create(self,
               storage_namespace: str,
               default_branch: str = "main",
               include_samples: bool = False,
               exist_ok: bool = False,
               **kwargs) -> Repository:
        """
        Create a new repository in lakeFS from this object
        :param storage_namespace: Repository's storage namespace
        :param default_branch: The default branch for the repository. If None, use server default name
        :param include_samples: Whether to include sample data in repository creation
        :param exist_ok: If False will throw an exception if a repository by this name already exists. Otherwise,
         return the existing repository without creating a new one
        :return: The lakeFS SDK object representing the repository
        :raises
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        repository_creation = lakefs_sdk.RepositoryCreation(name=self._id,
                                                            storage_namespace=storage_namespace,
                                                            default_branch=default_branch,
                                                            sample_data=include_samples)
        try:
            repo = self._client.sdk_client.repositories_api.create_repository(repository_creation, **kwargs)
            self._properties = RepositoryProperties(**repo.__dict__)
            return self
        except lakefs_sdk.exceptions.ApiException as e:
            if e.status == http.HTTPStatus.CONFLICT.value and exist_ok:  # Handle conflict 409
                try:
                    repo = self._client.sdk_client.repositories_api.get_repository(self._id)
                    self._properties = RepositoryProperties(**repo.__dict__)
                    return self
                except lakefs_sdk.exceptions.ApiException as ex:
                    _handle_api_exception(ex)
            _handle_api_exception(e)

    def delete(self) -> None:
        """
        Delete repository from lakeFS server
        :raises
            RepositoryNotFoundException if repository by this id does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        try:
            self._client.sdk_client.repositories_api.delete_repository(self._id)
        except lakefs_sdk.exceptions.ApiException as e:
            _handle_api_exception(e)

    def branch(self, branch_id: str) -> lakefs.branch.Branch:
        """
        Return a branch object using the current repository id and client
        :param branch_id: name of the branch
        """
        return lakefs.branch.Branch(self._id, branch_id, self._client)

    def commit(self, commit_id: str) -> Reference:
        """
        Return a reference object using the current repository id and client
        :param commit_id: id of the commit reference
        """
        return lakefs.reference.Reference(self._id, commit_id, self._client)

    def ref(self, ref_id: str) -> Reference:
        """
        Return a reference object using the current repository id and client
        :param ref_id: branch name, commit id or tag id
        """
        return lakefs.reference.Reference(self._id, ref_id, self._client)

    def tag(self, tag_id: str) -> lakefs.tag.Tag:
        """
        Return a tag object using the current repository id and client
        :param tag_id: name of the tag
        """
        return lakefs.tag.Tag(self._id, tag_id, self._client)

    @property
    def metadata(self) -> dict[str, str]:
        """
        Returns the repository metadata
        """
        try:
            return self._client.sdk_client.repositories_api.get_repository_metadata(repository=self._id)
        except lakefs_sdk.exceptions.ApiException as ex:
            _handle_api_exception(ex)

    @property
    def branches(self) -> BranchManager:
        """
        Returns a BranchManager object for this repository
        """
        return BranchManager()

    @property
    def tags(self) -> TagManager:
        """
        Returns a TagManager object for this repository
        """
        return TagManager()

    @property
    def properties(self) -> RepositoryProperties:
        if self._properties is None:
            try:
                repo = self._client.sdk_client.repositories_api.get_repository(self._id)
                self._properties = RepositoryProperties(**repo.__dict__)
            except lakefs_sdk.exceptions.ApiException as ex:
                _handle_api_exception(ex)
        return self._properties


def _handle_api_exception(e: lakefs_sdk.exceptions.ApiException):
    if isinstance(e, lakefs_sdk.exceptions.NotFoundException):
        raise RepositoryNotFoundException(e.status, e.reason) from e
    if isinstance(e, lakefs_sdk.exceptions.UnauthorizedException):
        raise NotAuthorizedException(e.status, e.reason) from e
    raise ServerException(e.status, e.reason) from e
