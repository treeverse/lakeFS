"""
Module containing lakeFS reference implementation
"""


class Object:
    """
    Class representing an object in lakeFS.
    """

    _repo_id: str
    _ref_id: str
    _path: str

    def __init__(self, repo_id: str, ref_id: str, path: str):
        self._repo_id = repo_id
        self._ref_id = ref_id
        self._path = path

    # TODO: Implements
