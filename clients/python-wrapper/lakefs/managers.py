"""
Managers module
"""

from typing import Optional, Generator, TypeVar, Generic, Callable, Any, get_args

from lakefs.reference import get_generator
from lakefs.tag import Tag
from lakefs.branch import Branch
from lakefs.client import DEFAULT_CLIENT, Client

T = TypeVar("T")


class BaseManager(Generic[T]):
    """
    Base class for lakeFS Managers
    """

    _repo_id: str
    _client: Client
    _list_func: Callable
    _class_type: Any

    def __init__(self, repository_id: str, list_func: Callable, client: Optional[Client] = DEFAULT_CLIENT):
        self._repo_id = repository_id
        self._client = client
        self._list_func = list_func
        self._class_type = get_args(self.__class__.__orig_bases__[0])[0]  # pylint: disable=no-member

    def list(self, max_amount: Optional[int] = None,
             after: Optional[str] = None, prefix: Optional[str] = None, **kwargs) -> Generator[T, None, None]:
        """
        Returns a generator listing for the type T on the given repository

        :param max_amount: Stop showing changes after this amount
        :param after: Return items after this value
        :param prefix: Return items prefixed with this value
        :raises:
            NotFoundException if repository does not exist
            NotAuthorizedException if user is not authorized to perform this operation
            ServerException for any other errors
        """
        for res in get_generator(self._list_func, self._repo_id,
                                 max_amount=max_amount, after=after, prefix=prefix, **kwargs):
            yield self._class_type(self._repo_id, res.id, client=self._client)


class TagManager(BaseManager[Tag]):
    """
    Manage tags listing for a given repository
    """

    def __init__(self, repository_id: str, client: Optional[Client] = DEFAULT_CLIENT):
        super().__init__(repository_id, client.sdk_client.tags_api.list_tags, client)


class BranchManager(BaseManager[Branch]):
    """
    Manage branches listing for a given repository
    """

    _repo_id: str
    _client: Client

    def __init__(self, repository_id: str, client: Optional[Client] = DEFAULT_CLIENT):
        super().__init__(repository_id, client.sdk_client.branches_api.list_branches, client)
