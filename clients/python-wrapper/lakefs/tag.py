"""
Module containing lakeFS tag implementation
"""

from __future__ import annotations
from typing import Optional

import lakefs_sdk

from lakefs.exceptions import api_exception_handler, LakeFSException, ConflictException
from lakefs.reference import Reference


class Tag(Reference):
    """
    Class representing a tag in lakeFS.
    """

    def create(self, source_ref_id: str | Reference, exist_ok: Optional[bool] = False) -> Tag:
        """
        Create a tag from the given source_ref_id

        :param source_ref_id: The reference id to create the tag on
        :param exist_ok: If True returns the existing Tag reference otherwise raises exception
        :return: A lakeFS SDK Tag object
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise NotFoundException: if source_ref_id doesn't exist on the lakeFS server
        :raise ServerException: for any other errors.
        """
        if isinstance(source_ref_id, Reference):
            source_ref_id = source_ref_id.id
        tag_creation = lakefs_sdk.TagCreation(id=self.id, ref=source_ref_id)

        def handle_conflict(e: LakeFSException):
            if not (isinstance(e, ConflictException) and exist_ok):
                return e
            return None

        with api_exception_handler(handle_conflict):
            self._client.sdk_client.tags_api.create_tag(self._repo_id, tag_creation)

        return self

    def delete(self) -> None:
        """
        Delete the tag from the lakeFS server

        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise NotFoundException: if source_ref_id doesn't exist on the lakeFS server
        :raise ServerException: for any other errors
        """
        with api_exception_handler():
            self._client.sdk_client.tags_api.delete_tag(self._repo_id, self.id)
            self._commit = None
