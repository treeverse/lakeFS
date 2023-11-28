"""
Module implementing import logic
"""

from lakefs.client import Client, DEFAULT_CLIENT


class ImportManager:
    """
    Manage an import operation on a given repository
    """

    def __init__(self, repository_id: str, reference_id: str, commit_message: str, metadata: dict = None,
                 client: Client = DEFAULT_CLIENT):
        self._client = client
        self._repo_id = repository_id
        self._ref_id = reference_id
        self._commit_message = commit_message
        self._metadata = metadata

    # TODO: Implement
