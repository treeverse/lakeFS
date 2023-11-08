# lakefs.exceptions
class LakeFSException(Exception):
    status_code: int
    message: str


# More specific "not found"s can inherit from this:
class NotFoundException(LakeFSException):
    pass


class NoAuthenticationFound(LakeFSException):
    pass


class NotAuthorizedException(LakeFSException):
    pass


class ServerException(LakeFSException):
    pass


class UnsupportedOperationException(LakeFSException):
    pass


class ObjectNotFoundException(NotFoundException, FileNotFoundError):
    pass


# raised when Object('...').create(mode='x') and object exists
class ObjectExistsException(LakeFSException, FileExistsError):
    pass


# Returned by Object.open() and Object.create() for compatibility with python
class PermissionException(NotAuthorizedException, PermissionError):
    pass
