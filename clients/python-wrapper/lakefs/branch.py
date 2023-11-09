"""
Module containing lakeFS branch implementation
"""

from lakefs.reference import Reference


class Branch(Reference):
    """
    Class representing a branch in lakeFS. This class should not be instantiated on its own. It should be created
    in the context of a Repository object.
    """

    def __init__(self) -> None:  # pylint: disable=W0231
        raise NotImplementedError("Object instantiation not allowed")
