"""
Module containing lakeFS reference implementation
"""

from lakefs.client import Client


class Reference:
    """
    Class representing a reference in lakeFS.
    """
    _client: Client
    repo_id: str
    id: str

    def __init__(self, client: Client, repo_id: str, ref_id: str) -> None:
        self._client = client
        self.repo_id = repo_id
        self.id = ref_id

    # TODO: Implement
