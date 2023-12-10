"""
Module containing lakeFS reference implementation
"""

from __future__ import annotations

from typing import Optional, Generator

import lakefs_sdk

from lakefs.models import Commit, Change, CommonPrefix, ObjectInfo, _OBJECT
from lakefs.client import Client, _BaseLakeFSObject
from lakefs.exceptions import api_exception_handler
from lakefs.object import StoredObject


class Reference(_BaseLakeFSObject):
    """
    Class representing a reference in lakeFS.
    """
    _repo_id: str
    _id: str
    _commit: Optional[Commit] = None

    def __init__(self, repo_id: str, ref_id: str, client: Optional[Client] = None) -> None:
        self._repo_id = repo_id
        self._id = ref_id
        super().__init__(client)

    @property
    def repo_id(self) -> str:
        """
        Return the repository id for this reference
        """
        return self._repo_id

    @property
    def id(self) -> str:
        """
        Returns the reference id
        """
        return self._id

    def objects(self,
                max_amount: Optional[int] = None,
                after: Optional[str] = None,
                prefix: Optional[str] = None,
                delimiter: Optional[str] = None,
                **kwargs) -> Generator[StoredObject | CommonPrefix]:
        """
        Returns an object generator for this reference, the generator can yield either a StoredObject or a CommonPrefix
        object depending on the listing parameters provided.

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :param delimiter: Group common prefixes by this delimiter
        :param kwargs: Additional Keyword Arguments to send to the server
        :raise NotFoundException: if this reference or other_ref does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """

        for res in generate_listing(self._client.sdk_client.objects_api.list_objects,
                                    repository=self._repo_id,
                                    ref=self._id,
                                    max_amount=max_amount,
                                    after=after,
                                    prefix=prefix,
                                    delimiter=delimiter,
                                    **kwargs):
            type_class = ObjectInfo if res.path_type == _OBJECT else CommonPrefix
            yield type_class(**res.dict())

    def log(self, max_amount: Optional[int] = None, **kwargs) -> Generator[Commit]:
        """
        Returns a generator of commits starting with this reference id

        :param max_amount: (Optional) limits the amount of results to return from the server
        :param kwargs: Additional Keyword Arguments to send to the server
        :raise NotFoundException: if reference by this id does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        for res in generate_listing(self._client.sdk_client.refs_api.log_commits, self._repo_id, self._id,
                                    max_amount=max_amount, **kwargs):
            yield Commit(**res.dict())

    def get_commit(self) -> Commit:
        """
        Returns the underlying commit referenced by this reference id

        :raise NotFoundException: if this reference does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        if self._commit is None:
            with api_exception_handler():
                commit = self._client.sdk_client.commits_api.get_commit(self._repo_id, self._id)
                self._commit = Commit(**commit.dict())
        return self._commit

    def diff(self,
             other_ref: str | Reference,
             max_amount: Optional[int] = None,
             after: Optional[str] = None,
             prefix: Optional[str] = None,
             delimiter: Optional[str] = None,
             **kwargs) -> Generator[Change]:
        """
        Returns a diff generator of changes between this reference and other_ref

        :param other_ref: The other ref to diff against
        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :param delimiter: Group common prefixes by this delimiter
        :param kwargs: Additional Keyword Arguments to send to the server
        :raise NotFoundException: if this reference or other_ref does not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        other_ref_id = other_ref
        if isinstance(other_ref, Reference):
            other_ref_id = other_ref.id
        for diff in generate_listing(self._client.sdk_client.refs_api.diff_refs,
                                     repository=self._repo_id,
                                     left_ref=self._id,
                                     right_ref=other_ref_id,
                                     after=after,
                                     max_amount=max_amount,
                                     prefix=prefix,
                                     delimiter=delimiter,
                                     **kwargs):
            yield Change(**diff.dict())

    def merge_into(self, destination_branch_id: str | Reference, **kwargs) -> str:
        """
        Merge this reference into destination branch

        :param destination_branch_id: The ID of the merge destination
        :param kwargs: Additional Keyword Arguments to send to the server
        :return: The reference id of the merge commit
        :raise NotFoundException: if reference by this id does not exist, or branch doesn't exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        if isinstance(destination_branch_id, Reference):
            destination_branch_id = destination_branch_id.id
        with api_exception_handler():
            merge = lakefs_sdk.Merge(**kwargs)
            res = self._client.sdk_client.refs_api.merge_into_branch(self._repo_id,
                                                                     self._id,
                                                                     destination_branch_id,
                                                                     merge=merge)
            return res.reference

    def object(self, path: str) -> StoredObject:  # pylint: disable=C0103
        """
        Returns an Object class representing a lakeFS object with this repo id, reference id and path

        :param path: The object's path
        """
        return StoredObject(self._repo_id, self._id, path, self._client)

    def __repr__(self):
        class_name = self.__class__.__name__
        return f'{class_name}(repository="{self.repo_id}", id="{self.id}")'


def generate_listing(func, *args, max_amount: Optional[int] = None, **kwargs):
    """
    Generic generator function, for lakefs-sdk listings functionality

    :param func: The listing function
    :param args: The function args
    :param max_amount: The max amount of objects to generate
    :param kwargs: The function kwargs
    :return: A generator based on the listing function
    """
    has_more = True
    with api_exception_handler():
        while has_more:
            page = func(*args, **kwargs)
            has_more = page.pagination.has_more
            kwargs["after"] = page.pagination.next_offset
            for res in page.results:
                yield res

                if max_amount is not None:
                    max_amount -= 1
                    if max_amount <= 0:
                        return
