"""
Module containing lakeFS branch implementation
"""

from __future__ import annotations

from typing import Optional, Generator, Iterable, Literal

import lakefs_sdk
from lakefs.client import Client
from lakefs.object import WriteableObject
from lakefs.object import StoredObject
from lakefs.import_manager import ImportManager
from lakefs.reference import Reference, generate_listing
from lakefs.models import Change
from lakefs.exceptions import api_exception_handler, ConflictException, LakeFSException, UnsupportedOperationException


class Branch(Reference):
    """
    Class representing a branch in lakeFS.
    """

    def get_commit(self):
        """
        For branches override the default _get_commit method to ensure we always fetch the latest head
        """
        self._commit = None
        return super().get_commit()

    def create(self, source_reference_id: str | Reference, exist_ok: bool = False) -> Branch:
        """
        Create a new branch in lakeFS from this object

        Example of creating a new branch:

        .. code-block:: python

            import lakefs

            branch = lakefs.repository("<repository_name>").branch("<branch_name>").create("<source_reference>")

        :param source_reference_id: The reference to create the branch from
        :param exist_ok: If False will throw an exception if a branch by this name already exists. Otherwise,
            return the existing branch without creating a new one
        :return: The lakeFS SDK object representing the branch
        :raise NotFoundException: if repo, branch or source reference id does not exist
        :raise ConflictException: if branch already exists and exist_ok is False
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """

        def handle_conflict(e: LakeFSException):
            if isinstance(e, ConflictException) and exist_ok:
                return None
            return e
        if isinstance(source_reference_id, Reference):
            source_reference_id = source_reference_id.id
        branch_creation = lakefs_sdk.BranchCreation(name=self._id, source=source_reference_id)
        with api_exception_handler(handle_conflict):
            self._client.sdk_client.branches_api.create_branch(self._repo_id, branch_creation)
        return self

    def head(self) -> Reference:
        """
        Get the commit reference this branch is pointing to

        :return: The commit reference this branch is pointing to
        :raise NotFoundException: if branch by this id does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
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
        :raise NotFoundException: if branch by this id does not exist
        :raise ForbiddenException: if commit is not allowed on this branch
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """

        commits_creation = lakefs_sdk.CommitCreation(message=message, metadata=metadata)
        with api_exception_handler():
            c = self._client.sdk_client.commits_api.commit(self._repo_id, self._id, commits_creation)
        return Reference(self._repo_id, c.id, self._client)

    def delete(self) -> None:
        """
        Delete branch from lakeFS server

        :raise NotFoundException: if branch or repository do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ForbiddenException: for branches that are protected
        :raise ServerException: for any other errors
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
        :raise NotFoundException: if branch by this id does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
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

    def uncommitted(self, max_amount: Optional[int] = None, after: Optional[str] = None, prefix: Optional[str] = None,
                    **kwargs) -> Generator[Change]:
        """
        Returns a diff generator of uncommitted changes on this branch

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :param kwargs: Additional Keyword Arguments to send to the server
        :raise NotFoundException: if branch or repository do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """

        for diff in generate_listing(self._client.sdk_client.branches_api.diff_branch,
                                     self._repo_id, self._id, max_amount=max_amount, after=after, prefix=prefix,
                                     **kwargs):
            yield Change(**diff.dict())

    def import_data(self, commit_message: str = "", metadata: Optional[dict] = None) -> ImportManager:
        """
        Import data to lakeFS

        :param metadata: metadata to attach to the commit
        :param commit_message: once the data is imported, a commit is created with this message. If default (empty)
            message is provided, uses the default server commit message for imports.
        :return: an ImportManager object
        """
        return ImportManager(self._repo_id, self._id, commit_message, metadata, self._client)

    def delete_objects(self, object_paths: str | StoredObject | Iterable[str | StoredObject]) -> None:
        """
        Delete objects from lakeFS

        This method can be used to delete single/multiple objects from branch. It accepts both str and StoredObject
        types as well as Iterables of these types.
        Using this method is more performant than sequentially calling delete on objects as it saves the back and forth
        from the server.

        This can also be used in combination with object listing. For example:

        .. code-block:: python

            import lakefs

            branch = lakefs.repository("<repository_name>").branch("<branch_name>")
            # list objects on a common prefix
            objs = branch.objects(prefix="my-object-prefix/", max_amount=100)
            # delete objects which have "foo" in their name
            branch.delete_objects([o.path for o in objs if "foo" in o.path])

        :param object_paths: a single path or an iterable of paths to delete
        :raise NotFoundException: if branch or repository do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        if isinstance(object_paths, str):
            object_paths = [object_paths]
        elif isinstance(object_paths, StoredObject):
            object_paths = [object_paths.path]
        elif isinstance(object_paths, Iterable):
            object_paths = [o.path if isinstance(o, StoredObject) else o for o in object_paths]
        with api_exception_handler():
            return self._client.sdk_client.objects_api.delete_objects(
                self._repo_id,
                self._id,
                lakefs_sdk.PathList(paths=object_paths)
            )

    def transact(self, commit_message: str) -> Transaction:
        """
        Create a transaction for multiple operations

        :param commit_message: once the transaction is committed, a commit is created with this message
        :return: a Transaction object to perform operations on
        """
        raise UnsupportedOperationException("Transaction not yet implemented")

    def reset_changes(self, path_type: Literal["common_prefix", "object", "reset"] = "reset",
                      path: Optional[str] = None) -> None:
        """
        Reset uncommitted changes (if any) on this branch

        :param path_type: the type of path to reset ('common_prefix', 'object', 'reset' - for all changes)
        :param path: the path to reset (optional) - if path_type is 'reset' this parameter is ignored
        :raise ValidationError: if path_type is not one of the allowed values
        :raise NotFoundException: if branch or repository do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """

        reset_creation = lakefs_sdk.ResetCreation(path=path, type=path_type)
        return self._client.sdk_client.branches_api.reset_branch(self._repo_id, self.id, reset_creation)


class Transaction(Branch):
    """
    Manage a transactions on a given branch (TBD)
    """

    def __init__(self, repository_id: str, branch_id: str, commit_message: str, client: Client = None):
        super().__init__(repository_id, branch_id, client)
        self._commit_message = commit_message

    # TODO: Implement and check if we are OK with transaction returning a branch
    #  with capabilities such as commit and transaction
