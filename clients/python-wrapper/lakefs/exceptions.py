"""
Exceptions module
"""
import http
from contextlib import contextmanager
from typing import Optional, Callable

import lakefs_sdk.exceptions


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
    Resource could not be found on lakeFS server
    """


class ForbiddenException(LakeFSException):
    """
    Operation not permitted
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


class ConflictException(LakeFSException):
    """
     Resource / request conflict
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


class RefNotFoundException(NotFoundException):
    """
    Raised when the reference could not be found in the lakeFS server
    """


_STATUS_CODE_TO_EXCEPTION = {
    http.HTTPStatus.UNAUTHORIZED.value: NotAuthorizedException,
    http.HTTPStatus.FORBIDDEN.value: ForbiddenException,
    http.HTTPStatus.NOT_FOUND.value: NotFoundException,
    http.HTTPStatus.METHOD_NOT_ALLOWED.value: UnsupportedOperationException,
    http.HTTPStatus.CONFLICT.value: ConflictException,
}


@contextmanager
def api_exception_handler(custom_handler: Optional[Callable[[LakeFSException], None]] = None):
    """
    Contexts which converts lakefs_sdk API exceptions to LakeFS exceptions and handles them
    :param custom_handler: Optional handler which can be used to provide custom behavior for specific exceptions
    """
    try:
        yield
    except lakefs_sdk.ApiException as e:
        lakefs_ex = _STATUS_CODE_TO_EXCEPTION.get(e.status, ServerException)(e.status, e.reason)
        if custom_handler is None:
            raise lakefs_ex from e

        custom_handler(lakefs_ex)
