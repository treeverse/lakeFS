"""
Module containing lakeFS repository implementation
"""

from __future__ import annotations

from typing import Optional, Generator

import lakefs_sdk

from lakefs.models import RepositoryProperties
from lakefs.tag import Tag
from lakefs.branch import Branch
from lakefs.client import Client, DEFAULT_CLIENT
from lakefs.exceptions import api_exception_handler, ConflictException, LakeFSException
from lakefs.reference import Reference, generate_listing


class Repository:
    """
    Class representing a Repository in lakeFS.
    The Repository object provides the context for the other objects that are found in it.
    Access to these objects should be done from this class
    """
    _client: Client
    _id: str
    _properties: RepositoryProperties = None

    def __init__(self, repository_id: str, client: Optional[Client] = DEFAULT_CLIENT) -> None:
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
        :param kwargs: Additional Keyword Arguments to send to the server
        :return: The lakeFS SDK object representing the repository
        :raises:
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        repository_creation = lakefs_sdk.RepositoryCreation(name=self._id,
                                                            storage_namespace=storage_namespace,
                                                            default_branch=default_branch,
                                                            sample_data=include_samples)

        def handle_conflict(e: LakeFSException):
            if isinstance(e, ConflictException) and exist_ok:
                with api_exception_handler():
                    get_repo = self._client.sdk_client.repositories_api.get_repository(self._id)
                    self._properties = RepositoryProperties(**get_repo.dict())
                    return None
            return e

        with api_exception_handler(handle_conflict):
            repo = self._client.sdk_client.repositories_api.create_repository(repository_creation, **kwargs)
            self._properties = RepositoryProperties(**repo.dict())
        return self

    def delete(self) -> None:
        """
        Delete repository from lakeFS server

        :raises:
            NotFoundException if repository by this id does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        with api_exception_handler():
            self._client.sdk_client.repositories_api.delete_repository(self._id)

    def branch(self, branch_id: str) -> Branch:
        """
        Return a branch object using the current repository id and client
        :param branch_id: name of the branch
        """
        return Branch(self._id, branch_id, self._client)

    def commit(self, commit_id: str) -> Reference:
        """
        Return a reference object using the current repository id and client
        :param commit_id: id of the commit reference
        """
        return Reference(self._id, commit_id, self._client)

    def ref(self, ref_id: str) -> Reference:
        """
        Return a reference object using the current repository id and client
        :param ref_id: branch name, commit id or tag id
        """
        return Reference(self._id, ref_id, self._client)

    def tag(self, tag_id: str) -> Tag:
        """
        Return a tag object using the current repository id and client
        :param tag_id: name of the tag
        """
        return Tag(self._id, tag_id, self._client)

    def branches(self, max_amount: Optional[int] = None,
                 after: Optional[str] = None, prefix: Optional[str] = None, **kwargs) -> Generator[Branch]:
        """
        Returns a generator listing for branches on the given repository

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :param kwargs: Additional Keyword Arguments to send to the server
        :raises:
            NotFoundException if repository does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """

        for res in generate_listing(self._client.sdk_client.branches_api.list_branches, self._id,
                                    max_amount=max_amount, after=after, prefix=prefix, **kwargs):
            yield Branch(self._id, res.id, client=self._client)

    def tags(self, max_amount: Optional[int] = None,
             after: Optional[str] = None, prefix: Optional[str] = None, **kwargs) -> Generator[Tag]:
        """
        Returns a generator listing for tags on the given repository

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :param kwargs: Additional Keyword Arguments to send to the server
        :raises:
            NotFoundException if repository does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """

        for res in generate_listing(self._client.sdk_client.tags_api.list_tags, self._id,
                                    max_amount=max_amount, after=after, prefix=prefix, **kwargs):
            yield Tag(self._id, res.id, client=self._client)

    @property
    def metadata(self) -> dict[str, str]:
        """
        Returns the repository metadata
        """
        with api_exception_handler():
            return self._client.sdk_client.repositories_api.get_repository_metadata(repository=self._id)

    @property
    def properties(self) -> RepositoryProperties:
        """
        Return the repositories properties object
        """
        if self._properties is None:
            with api_exception_handler():
                repo = self._client.sdk_client.repositories_api.get_repository(self._id)
                self._properties = RepositoryProperties(**repo.dict())
                return self._properties

        return self._properties
