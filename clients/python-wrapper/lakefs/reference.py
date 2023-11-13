"""
Module containing lakeFS reference implementation
"""
from typing import Optional

from lakefs.client import Client, DefaultClient


class Reference:
    """
    Class representing a reference in lakeFS.
    """
    _client: Client
    repo_id: str
    id: str

    def __init__(self, repo_id: str, ref_id: str, client: Optional[Client] = DefaultClient) -> None:
        self._client = client
        self.repo_id = repo_id
        self.id = ref_id

    # TODO: Implement
