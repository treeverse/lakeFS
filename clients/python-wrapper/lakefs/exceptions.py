"""
SDK Wrapper exceptions module
"""


class LakeFSException(Exception):
    """
    Base exception for all SDK Wrapper exceptions
    """
    status_code: int
    reason: str

    def __init__(self, status=None, reason=None):
        self.status_code = status
        self.message = reason


class NotFoundException(LakeFSException):
    """
    Base class for NotFound exceptions. More specific "not found"s can inherit from this
    """


class NoAuthenticationFound(LakeFSException):
    """
    Raised when no authentication method could be found on Client instantiation
    """


class NotAuthorizedException(LakeFSException):
    """
    User not authorized to perform operation
    """


class ServerException(LakeFSException):
    """
    Generic exception when no other exception is applicable
    """


class UnsupportedOperationException(LakeFSException):
    """
    Operation not supported by lakeFS server or SDK wrapper
    """


class ObjectNotFoundException(NotFoundException, FileNotFoundError):
    """
    Raised when the currently used object no longer exist in the lakeFS server
    """


class ObjectExistsException(LakeFSException, FileExistsError):
    """
    Raised when Object('...').create(mode='x') and object exists
    """


class PermissionException(NotAuthorizedException, PermissionError):
    """
    Raised by Object.open() and Object.create() for compatibility with python
    """


class RepositoryNotFoundException(NotFoundException):
    """
    Raised when the currently used repository object no longer exists in the lakeFS server
    """
