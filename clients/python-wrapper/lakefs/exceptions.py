"""
Exceptions module
"""
import http
import json
from contextlib import contextmanager
from typing import Optional, Callable
import logging

import lakefs_sdk.exceptions
from urllib3 import HTTPResponse


class LakeFSException(Exception):
    """
    Base exception for all SDK exceptions
    """


class ServerException(LakeFSException):
    """
    Generic exception when no other exception is applicable
    """
    status_code: int
    reason: str
    body: dict

    def __init__(self, status=None, reason=None, body=None):
        self.status_code = status
        self.reason = reason
        if body is not None:
            try:  # Try to get message from body
                self.body = json.loads(body)
            except json.JSONDecodeError:
                logging.debug("failed to decode response body: status (%s), reason (%s), body (%s)",
                              status, reason, body)
                self.body = {}
        else:
            self.body = {}

    def __str__(self):
        return f"code: {self.status_code}, reason: {self.reason}, body: {self.body}"


class NotFoundException(ServerException):
    """
    Resource could not be found on lakeFS server
    """


class ForbiddenException(ServerException):
    """
    Operation not permitted
    """


class NoAuthenticationFound(LakeFSException):
    """
    Raised when no authentication method could be found on Client instantiation
    """

class UnsupportedCredentialsProviderType(LakeFSException):
    """
    Raised when the credentials provider type is not supported
    """

class InvalidEnvVarFormat(LakeFSException):
    """
    Raised when the passed env var is not of expected format
    """

class BadRequestException(ServerException):
    """
    Bad Request
    """


class NotAuthorizedException(ServerException):
    """
    User not authorized to perform operation
    """


class UnsupportedOperationException(ServerException):
    """
    Operation not supported by lakeFS server or SDK
    """


class ConflictException(ServerException):
    """
     Resource / request conflict
    """


class ObjectNotFoundException(NotFoundException, FileNotFoundError):
    """
    Raised when the currently used object no longer exist in the lakeFS server
    """


class ObjectExistsException(ServerException, FileExistsError):
    """
    Raised when Object('...').create(mode='x') and object exists
    """


class PermissionException(NotAuthorizedException, PermissionError):
    """
    Raised by Object.open() and Object.create() for compatibility with python
    """


class InvalidRangeException(ServerException, OSError):
    """
    Raised when the reference could not be found in the lakeFS server
    """


class ImportManagerException(LakeFSException):
    """
    Import manager exceptions that are not originated from the SDK
    """


class TransactionException(LakeFSException):
    """
    Exceptions during the transaction commit logic
    """


_STATUS_CODE_TO_EXCEPTION = {
    http.HTTPStatus.BAD_REQUEST.value: BadRequestException,
    http.HTTPStatus.UNAUTHORIZED.value: NotAuthorizedException,
    http.HTTPStatus.FORBIDDEN.value: ForbiddenException,
    http.HTTPStatus.NOT_FOUND.value: NotFoundException,
    http.HTTPStatus.METHOD_NOT_ALLOWED.value: UnsupportedOperationException,
    http.HTTPStatus.CONFLICT.value: ConflictException,
    http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value: InvalidRangeException
}


@contextmanager
def api_exception_handler(custom_handler: Optional[Callable[[LakeFSException], LakeFSException]] = None):
    """
    Contexts which converts lakefs_sdk API exceptions to LakeFS exceptions and handles them.

    :param custom_handler: Optional handler which can be used to provide custom behavior for specific exceptions.
        If custom_handler returns an exception, this function will raise the exception at the end of the
        custom_handler invocation.
    """
    try:
        yield
    except lakefs_sdk.ApiException as e:
        lakefs_ex = _STATUS_CODE_TO_EXCEPTION.get(e.status, ServerException)(e.status, e.reason, e.body)
        if custom_handler is not None:
            lakefs_ex = custom_handler(lakefs_ex)

        if lakefs_ex is not None:
            raise lakefs_ex from e


def handle_http_error(resp: HTTPResponse) -> None:
    """
    Handles http response and raises the appropriate lakeFS exception if needed

    :param resp: The response to parse
    """
    if not http.HTTPStatus.OK <= resp.status < http.HTTPStatus.MULTIPLE_CHOICES:
        lakefs_ex = _STATUS_CODE_TO_EXCEPTION.get(resp.status, ServerException)(resp.status, resp.reason, resp.data)
        raise lakefs_ex
