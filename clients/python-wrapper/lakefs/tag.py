"""
Module containing lakeFS tag implementation
"""

from lakefs.reference import Reference


class Tag(Reference):
    """
    Class representing a tag in lakeFS. This class should not be instantiated on its own. It should be created
    in the context of a Repository object.
    """

    def __init__(self) -> None:  # pylint: disable=W0231
        raise NotImplementedError("Object instantiation not allowed")
