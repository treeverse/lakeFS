"""
Module containing lakeFS branch implementation
"""

from __future__ import annotations

import uuid
import warnings
from contextlib import contextmanager
from typing import Optional, Generator, Iterable, Literal, Dict

import lakefs_sdk
from lakefs.client import Client
from lakefs.object import WriteableObject
from lakefs.object import StoredObject
from lakefs.import_manager import ImportManager
from lakefs.reference import Reference, ReferenceType, generate_listing
from lakefs.models import Change, Commit
from lakefs.tag import Tag
from lakefs.exceptions import (
    api_exception_handler,
    ConflictException,
    LakeFSException,
    TransactionException,
)


class LakeFSDeprecationWarning(Warning):
    """
    Warning about use of a deprecated lakeFS or client feature. Unlike
    `DeprecationWarning`, this class is displayed by default. See
    `default warning filter <https://docs.python.org/3/library/warnings.html#default-warning-filter>`_
    for how to disable it.
    """


class _BaseBranch(Reference):

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


class Branch(_BaseBranch):
    """
    Class representing a branch in lakeFS.
    """

    def __init__(self, repository_id: str, branch_id: str, client: Optional[Client] = None):
        super().__init__(repository_id, reference_id=branch_id, client=client)

    def get_commit(self):
        """
        For branches override the default _get_commit method to ensure we always fetch the latest head
        """
        self._commit = None
        return super().get_commit()

    def cherry_pick(self, reference: ReferenceType, parent_number: Optional[int] = None) -> Commit:
        """
        Cherry-pick a given reference onto the branch.

        :param reference: ID of the reference to cherry-pick.
        :param parent_number: When cherry-picking a merge commit, the parent number (starting from 1)
            with which to perform the diff. The default branch is parent 1.
        :return: The cherry-picked commit at the head of the branch.
        :raise NotFoundException: If either the repository or target reference do not exist.
        :raise NotAuthorizedException: If the user is not authorized to perform this operation.
        :raise ServerException: For any other errors.
        """
        ref = reference if isinstance(reference, str) else reference.id
        cherry_pick_creation = lakefs_sdk.CherryPickCreation(ref=ref, parent_number=parent_number)
        with api_exception_handler():
            res = self._client.sdk_client.branches_api.cherry_pick(self._repo_id, self._id, cherry_pick_creation)
            return Commit(**res.dict())

    def create(self, source_reference: ReferenceType, exist_ok: bool = False, **kwargs) -> Branch:
        """
        Create a new branch in lakeFS from this object

        Example of creating a new branch:

        .. code-block:: python

            import lakefs

            branch = lakefs.repository("<repository_name>").branch("<branch_name>").create("<source_reference>")

        :param source_reference: The reference to create the branch from (reference ID, object or Commit object)
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

        reference_id = source_reference if isinstance(source_reference, str) else source_reference.id
        branch_creation = lakefs_sdk.BranchCreation(name=self._id, source=reference_id, **kwargs)
        with api_exception_handler(handle_conflict):
            self._client.sdk_client.branches_api.create_branch(self._repo_id, branch_creation)
        return self

    @property
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

    def commit(self, message: str, metadata: dict = None, **kwargs) -> Reference:
        """
        Commit changes on the current branch

        :param message: Commit message
        :param metadata: Metadata to attach to the commit
        :param kwargs: Additional Keyword Arguments for commit creation
        :return: The new reference after the commit
        :raise NotFoundException: if branch by this id does not exist
        :raise ForbiddenException: if commit is not allowed on this branch
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        commits_creation = lakefs_sdk.CommitCreation(message=message, metadata=metadata, **kwargs)

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

    def revert(self, reference: Optional[ReferenceType], parent_number: int = 0, *,
               reference_id: Optional[str] = None) -> Commit:
        """
        revert the changes done by the provided reference on the current branch

        :param reference_id: (Optional) The reference ID to revert

            .. deprecated:: 0.4.0
                Use ``reference`` instead.

        :param parent_number: when reverting a merge commit, the parent number (starting from 1) relative to which to
            perform the revert. The default for non merge commits is 0
        :param reference: the reference to revert
        :return: The commit created by the revert
        :raise NotFoundException: if branch by this id does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        if parent_number < 0:
            raise ValueError("parent_number must be a non-negative integer")

        if reference_id is not None:
            warnings.warn(
                "reference_id is deprecated, please use the `reference` argument.", LakeFSDeprecationWarning
            )
            # We show the error in case both are provided only after showing the deprecation warning, in order
            # for the user to have the most contextual clarity.
            if reference is not None:
                raise ValueError("`reference_id` and `reference` both provided "
                                 "Use only the `reference` argument.")

        # Handle reference_id as a deprecated alias to reference.
        reference = reference or reference_id
        if reference is None:
            raise ValueError("reference to revert must be specified")
        ref = reference if isinstance(reference, str) else reference.id

        with api_exception_handler():
            self._client.sdk_client.branches_api.revert_branch(
                self._repo_id,
                self._id,
                lakefs_sdk.RevertCreation(ref=ref, parent_number=parent_number)
            )
            commit = self._client.sdk_client.commits_api.get_commit(self._repo_id, self._id)
            return Commit(**commit.dict())

    def import_data(self, commit_message: str = "", metadata: Optional[dict] = None) -> ImportManager:
        """
        Import data to lakeFS

        :param metadata: metadata to attach to the commit
        :param commit_message: once the data is imported, a commit is created with this message. If default (empty)
            message is provided, uses the default server commit message for imports.
        :return: an ImportManager object
        """
        return ImportManager(self._repo_id, self._id, commit_message, metadata, self._client)

    @contextmanager
    def transact(self, commit_message: str = "", commit_metadata: Optional[Dict] = None,
                 delete_branch_on_error: bool = True, tag: Optional[str] = None) -> _Transaction:
        """
        Create a transaction for multiple operations.
        Transaction allows for multiple modifications to be performed atomically on a branch,
        similar to a database transaction.
        It ensures that the branch remains unaffected until the transaction is successfully completed.
        The process includes:

        1. Creating an ephemeral branch from this branch
        2. Perform object operations on ephemeral branch
        3. Commit changes
        4. Merge back to source branch
        5. Delete ephemeral branch

        Using a transaction the code for this flow will look like this:

        .. code-block:: python

            import lakefs

            branch = lakefs.repository("<repository_name>").branch("<branch_name>")
            with branch.transact(commit_message="my transaction") as tx:
                for obj in tx.objects(prefix="prefix_to_delete/"):  # Delete some objects
                    obj.delete()

                # Create new object
                tx.object("new_object").upload("new object data")

        Note that unlike database transactions, lakeFS transaction does not take a "lock" on the branch, and therefore
        the transaction might fail due to changes in source branch after the transaction was created.

        :param commit_message: once the transaction is committed, a commit is created with this message
        :param commit_metadata: user metadata for the transaction commit
        :param delete_branch_on_error: Defaults to True. Ensures ephemeral branch is deleted on error.
        :param tag: Optional tag name to create after the transaction is successfully completed
        :return: a Transaction object to perform the operations on
        """
        with Transaction(self._repo_id, self._id, commit_message, commit_metadata, delete_branch_on_error,
                         self._client, tag) as tx:
            yield tx


class _Transaction(_BaseBranch):
    @staticmethod
    def _get_tx_name() -> str:
        return f"tx-{uuid.uuid4()}"  # Don't rely on source branch name as this might exceed valid branch length

    def __init__(self, repository_id: str, branch_id: str, commit_message: str = "",
                 commit_metadata: Optional[Dict] = None, client: Client = None, tag: Optional[str] = None):
        self._commit_message = commit_message
        self._commit_metadata = commit_metadata
        self._source_branch = branch_id
        self._tag = tag

        tx_name = self._get_tx_name()
        self._tx_branch = Branch(repository_id, tx_name, client).create(branch_id, hidden=True)
        super().__init__(repository_id, tx_name, client)

    @property
    def source_id(self) -> str:
        """
        Returns the source branch ID the transaction is associated to
        """
        return self._source_branch

    @property
    def commit_message(self) -> str:
        """
        Return the commit message configured for this transaction completion
        """
        return self._commit_message

    @commit_message.setter
    def commit_message(self, message: str) -> None:
        """
        Set the commit message for this transaction completion
        :param message: The commit message to use on the transaction merge commit
        """
        self._commit_message = message

    @property
    def commit_metadata(self) -> Optional[Dict]:
        """
        Return the commit metadata configured for this transaction completion
        """
        return self._commit_metadata

    @commit_metadata.setter
    def commit_metadata(self, metadata: Optional[Dict]) -> None:
        """
        Set the commit metadata for this transaction completion
        :param metadata: The metadata to use on the transaction merge commit
        """
        self._commit_metadata = metadata

    @property
    def tag(self) -> Optional[str]:
        """
        Return the tag name configured for this transaction completion
        """
        return self._tag

    @tag.setter
    def tag(self, tag: str) -> None:
        """
        Set the tag name for this transaction completion
        :param tag: The tag name to create after the transaction is successfully completed
        """
        self._tag = tag


class Transaction:
    """
    Manage a transaction on a given branch

    The transaction creates an ephemeral branch from the source branch. The transaction can then be used to perform
    operations on the branch which will later be merged back into the source branch.
    Currently, transaction is supported only as a context manager.
    """

    def __init__(self, repository_id: str, branch_id: str, commit_message: str = "",
                 commit_metadata: Optional[Dict] = None, delete_branch_on_error: bool = True, client: Client = None,
                 tag: Optional[str] = None):
        self._repo_id = repository_id
        self._commit_message = commit_message
        self._commit_metadata = commit_metadata
        self._source_branch = branch_id
        self._client = client
        self._tx = None
        self._tx_branch = None
        self._cleanup_branch = delete_branch_on_error
        self._tag = tag

    def __enter__(self):
        self._tx = _Transaction(self._repo_id, self._source_branch, self._commit_message, self._commit_metadata,
                                self._client, self._tag)
        self._tx_branch = Branch(self._repo_id, self._tx.id, self._client)
        return self._tx

    def __exit__(self, typ, value, traceback) -> bool:
        if typ is not None:  # Perform only cleanup in case exception occurred
            if self._cleanup_branch:
                self._tx_branch.delete()
            return False  # Raise the underlying exception

        try:
            self._tx_branch.commit(message=self._tx.commit_message, metadata=self._tx.commit_metadata)
            merge_commit_id = self._tx_branch.merge_into(
                self._source_branch, message=f"Merge transaction {self._tx.id} to branch")
            self._tx_branch.delete()
        except LakeFSException as e:
            if self._cleanup_branch:
                self._tx_branch.delete()
            raise TransactionException(f"Failed committing transaction {self._tx.id}: {e}") from e

        if self._tx.tag:
            tag = Tag(self._repo_id, self._tx.tag, self._client)
            tag.create(merge_commit_id)
        return False
