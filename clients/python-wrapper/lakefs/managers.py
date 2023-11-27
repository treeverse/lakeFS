"""
Managers module
"""
from __future__ import annotations

from typing import Optional, Generator

from lakefs.reference import generate_listing
from lakefs.tag import Tag
from lakefs.branch import Branch
from lakefs.client import DEFAULT_CLIENT, Client


class TagManager:
    """
    Manage tags listing for a given repository
    """

    _repo_id: str
    _client: Client

    def __init__(self, repository_id: str, client: Optional[Client] = DEFAULT_CLIENT):
        self._repo_id = repository_id
        self._client = client

    def list(self, max_amount: Optional[int] = None,
             after: Optional[str] = None, prefix: Optional[str] = None, **kwargs) -> Generator[Tag]:
        """
        Returns a generator listing for tags on the given repository

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :raises:
            NotFoundException if repository does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """

        for res in generate_listing(self._client.sdk_client.tags_api.list_tags, self._repo_id,
                                    max_amount=max_amount, after=after, prefix=prefix, **kwargs):
            yield Tag(self._repo_id, res.id, client=self._client)


class BranchManager:
    """
    Manage branches listing for a given repository
    """

    def __init__(self, repository_id: str, client: Optional[Client] = DEFAULT_CLIENT):
        self._repo_id = repository_id
        self._client = client

    def list(self, max_amount: Optional[int] = None,
             after: Optional[str] = None, prefix: Optional[str] = None, **kwargs) -> Generator[Branch]:
        """
        Returns a generator listing for branches on the given repository

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :raises:
            NotFoundException if repository does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """

        for res in generate_listing(self._client.sdk_client.branches_api.list_branches, self._repo_id,
                                    max_amount=max_amount, after=after, prefix=prefix, **kwargs):
            yield Branch(self._repo_id, res.id, client=self._client)
