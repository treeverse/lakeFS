"""
Module containing lakeFS reference implementation
"""

from lakefs.client import Client


class _BaseRef:
    """
    Base class for all reference classes, maintains the attributes common to all reference types.
    """
    _client: Client
    _repo_id: str
    _id: str

    def __init__(self, client: Client, repo_id: str, ref_id: str) -> None:
        self._init_base(client, repo_id, ref_id)

    def _init_base(self, client: Client, repo_id: str, ref_id: str) -> None:
        self._client = client
        self._repo_id = repo_id
        self._id = ref_id


class Reference(_BaseRef):
    """
    Class representing a reference in lakeFS. This class should not be instantiated on its own. It should be created
    in the context of a Repository object.
    """

    def __init__(self) -> None:  # pylint: disable=W0231
        raise NotImplementedError("Object instantiation not allowed")
