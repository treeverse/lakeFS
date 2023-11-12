"""
Module containing lakeFS repository implementation
"""

import http
from typing import Optional

import lakefs_sdk

import lakefs.tag
import lakefs.branch
from lakefs.branch_manager import BranchManager
from lakefs.client import Client, DefaultClient
from lakefs.exceptions import RepositoryNotFoundException, NotAuthorizedException, ServerException
from lakefs.reference import Reference
from lakefs.tag_manager import TagManager


class Repository:
    """
    Class representing a Repository in lakeFS.
    The Repository object provides the context for the other objects that are found in it.
    Access to these objects should be done from this class
    """
    _client: Client
    _id: str

    class _Reference(Reference):
        """
        Internal class which enables instantiation of the Reference class
        """

        def __init__(self, client: Client, repo_id: str, ref_id: str) -> None:
            super()._init_base(client, repo_id, ref_id)

    class _Tag(lakefs.tag.Tag):
        """
        Internal class which enables instantiation of the Tag class
        """

        def __init__(self, client: Client, repo_id: str, tag_id: str) -> None:
            super()._init_base(client, repo_id, tag_id)

    class _Branch(lakefs.branch.Branch):
        """
        Internal class which enables instantiation of the Branch class
        """

        def __init__(self, client: Client, repo_id: str, branch_id: str) -> None:
            super()._init_base(client, repo_id, branch_id)

    def __init__(self, repository_id: str, client: Optional[Client] = DefaultClient) -> None:
        self._id = repository_id
        self._client = client

    def create(self,
               storage_namespace: str,
               default_branch: str = "main",
               include_samples: bool = False,
               exist_ok: bool = False,
               **kwargs) -> lakefs_sdk.Repository:
        """
        Create a new repository in lakeFS from this object
        :param storage_namespace: Repository's storage namespace
        :param default_branch: The default branch for the repository. If None, use server default name
        :param include_samples: Whether to include sample data in repository creation
        :param exist_ok: If False will throw an exception if a repository by this name already exists. Otherwise,
         return the existing repository without creating a new one
        :return: The lakefs SDK object representing the repository
        :raises
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        repository_creation = lakefs_sdk.RepositoryCreation(name=self._id,
                                                            storage_namespace=storage_namespace,
                                                            default_branch=default_branch,
                                                            sample_data=include_samples)
        try:
            return self._client.sdk_client.repositories_api.create_repository(repository_creation, **kwargs)
        except lakefs_sdk.exceptions.ApiException as e:
            if isinstance(e, lakefs_sdk.exceptions.UnauthorizedException):
                raise NotAuthorizedException(e.status, e.reason) from e
            if e.status == http.HTTPStatus.CONFLICT.value and exist_ok:  # Handle conflict 409
                return self._client.sdk_client.repositories_api.get_repository(self._id)
            raise ServerException(e.status, e.reason) from e

    def delete(self) -> None:
        """
        Delete repository from lakeFS server
        :raises
            NotFoundException if repository by this id does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        try:
            self._client.sdk_client.repositories_api.delete_repository(self._id)
        except lakefs_sdk.exceptions.ApiException as e:
            if isinstance(e, lakefs_sdk.exceptions.NotFoundException):
                raise RepositoryNotFoundException(e.status, e.reason) from e
            if isinstance(e, lakefs_sdk.exceptions.UnauthorizedException):
                raise NotAuthorizedException(e.status, e.reason) from e
            raise ServerException(e.status, e.reason) from e

    def Branch(self, branch_id: str) -> lakefs.branch.Branch:  # pylint: disable=C0103
        """
        Return a branch object using the current repository id and client
        :param branch_id: name of the branch
        """
        return Repository._Branch(self._client, self._id, branch_id)

    def Commit(self, commit_id: str) -> Reference:  # pylint: disable=C0103
        """
        Return a reference object using the current repository id and client
        :param commit_id: id of the commit reference
        """
        return Repository._Reference(self._client, self._id, commit_id)

    def Ref(self, ref_id: str) -> Reference:  # pylint: disable=C0103
        """
        Return a reference object using the current repository id and client
        :param ref_id: branch name, commit id or tag id
        """
        return Repository._Reference(self._client, self._id, ref_id)

    def Tag(self, tag_id: str) -> lakefs.tag.Tag:  # pylint: disable=C0103
        """
        Return a tag object using the current repository id and client
        :param tag_id: name of the tag
        """
        return Repository._Tag(self._client, self._id, tag_id)

    @property
    def metadata(self) -> dict[str, str]:
        """
        Returns the repository metadata
        """
        return self._client.sdk_client.repositories_api.get_repository_metadata(repository=self._id)

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
