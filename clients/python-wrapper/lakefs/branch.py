"""
Module containing lakeFS branch implementation
"""
from __future__ import annotations

from typing import Optional

import lakefs_sdk

from lakefs.object import WriteableObject
from lakefs.object_manager import WriteableObjectManager
from lakefs.reference import Reference
from lakefs.exceptions import api_exception_handler, ConflictException, LakeFSException


class Branch(Reference):
    """
    Class representing a branch in lakeFS.
    """

    def _get_commit(self):
        """
        For branches override the default _get_commit method to ensure we always fetch the latest head
        """
        self._commit = None
        return super()._get_commit()

    def create(self, source_reference_id: str | Reference, exist_ok: bool = False) -> Branch:
        """
        Create a new branch in lakeFS from this object

        :param source_reference_id: The reference to create the branch from
        :param exist_ok: If False will throw an exception if a branch by this name already exists. Otherwise,
            return the existing branch without creating a new one
        :return: The lakeFS SDK object representing the branch
        :raises:
            NotFoundException if repo, branch or source reference id does not exist
            ConflictException if branch already exists and exist_ok is False
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """

        def handle_conflict(e: LakeFSException):
            if isinstance(e, ConflictException) and exist_ok:
                return None
            return e

        branch_creation = lakefs_sdk.BranchCreation(name=self._id, source=str(source_reference_id))
        with api_exception_handler(handle_conflict):
            self._client.sdk_client.branches_api.create_branch(self._repo_id, branch_creation)
        return self

    def head(self) -> Reference:
        """
        Get the commit reference this branch is pointing to

        :return: The commit reference this branch is pointing to
        :raises:
            NotFoundException if branch by this id does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        with api_exception_handler():
            branch = self._client.sdk_client.branches_api.get_branch(self._repo_id, self._id)
        return Reference(self._repo_id, branch.commit_id, self._client)

    def commit(self, message: str, metadata: dict = None) -> Reference:
        """
        Commit changes on the current branch

        :param message: Commit message
        :param metadata: Metadata to attach to the commit
        :return: The new reference after the commit
        :raises:
            NotFoundException if branch by this id does not exist
            ForbiddenException if commit is not allowed on this branch
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """

        commits_creation = lakefs_sdk.CommitCreation(message=message, metadata=metadata)
        with api_exception_handler():
            c = self._client.sdk_client.commits_api.commit(self._repo_id, self._id, commits_creation)
        return Reference(self._repo_id, c.id, self._client)

    def delete(self) -> None:
        """
        Delete branch from lakeFS server

        :raises:
            NotFoundException if branch or repository do not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ForbiddenException for branches that are protected
            ServerException for any other errors
        """
        with api_exception_handler():
            return self._client.sdk_client.branches_api.delete_branch(self._repo_id, self._id)

    def revert(self, reference_id: str, parent_number: Optional[int] = None) -> None:
        """
        revert the changes done by the provided reference on the current branch

        :param parent_number: when reverting a merge commit, the parent number (starting from 1) relative to which to
            perform the revert.
        :param reference_id: the reference to revert
        :return: The new reference after the revert
        :raises:
            NotFoundException if branch by this id does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        if parent_number is None:
            parent_number = 0
        elif parent_number <= 0:
            raise ValueError("parent_number must be a positive integer")

        with api_exception_handler():
            return self._client.sdk_client.branches_api.revert_branch(
                self._repo_id,
                self._id,
                lakefs_sdk.RevertCreation(ref=reference_id, parent_number=parent_number)
            )

    def object(self, path: str) -> WriteableObject:
        """
        Returns a writable object using the current repo id, reference and path

        :param path: The object's path
        """

        return WriteableObject(self.repo_id, self._id, path, client=self._client)

    @property
    def objects(self) -> WriteableObjectManager:
        """
        Return an Objects object using the current repository id and client
        """
        return WriteableObjectManager()
