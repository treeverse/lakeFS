"""
Import module provides a simpler interface to the lakeFS SDK import functionality
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Optional, Dict, List

import lakefs_sdk

from lakefs.models import ImportStatus, _OBJECT, _COMMON_PREFIX
from lakefs.client import Client, _BaseLakeFSObject
from lakefs.exceptions import ImportManagerException, api_exception_handler


class ImportManager(_BaseLakeFSObject):
    """
    ImportManager provides an easy-to-use interface to perform imports with multiple sources.
    It provides both synchronous and asynchronous functionality allowing the user to start an import process,
    continue executing logic and poll for the import completion.

    ImportManager usage example:

    .. code-block:: python

        import lakefs

        branch = lakefs.repository("<repository_name>").branch("<branch_name>")
        mgr = branch.import_data(commit_message="my imported data", metadata={"foo": "bar"})

        # add sources for import
        mgr.prefix(object_store_uri="s3://import-bucket/data1/",
                   destination="import-prefix/").object(object_store_uri="s3://import-bucket/data2/imported_file",
                                                        destination="import-prefix/imported_file")
        # start import and wait
        mgr.run()

    """
    _repo_id: str
    _branch_id: str
    _in_progress: bool = False
    _import_id: str = None
    commit_message: str
    commit_metadata: Optional[Dict]
    sources: List[lakefs_sdk.ImportLocation]

    def __init__(self, repository_id: str, branch_id: str, commit_message: str = "",
                 commit_metadata: Optional[Dict] = None, client: Optional[Client] = None) -> None:
        self._repo_id = repository_id
        self._branch_id = branch_id
        self.commit_message = commit_message
        self.commit_metadata = commit_metadata
        self.sources = []
        super().__init__(client)

    @property
    def import_id(self) -> str:
        """
        Returns the id of the current import process
        """
        return self._import_id

    def _append_source(self, import_location: lakefs_sdk.ImportLocation):
        if self._import_id is not None:
            raise ImportManagerException("Cannot add additional sources to an already started import")

        self.sources.append(import_location)

    def prefix(self, object_store_uri: str, destination: str) -> ImportManager:
        """
        Creates a new import source of type "common_prefix" and adds it to the list of sources

        :param object_store_uri: The URI from which to import the objects
        :param destination: The destination prefix relative to the branch
        :return: The ImportManager instance (self) after update, to allow operations chaining
        """
        self._append_source(lakefs_sdk.ImportLocation(type=_COMMON_PREFIX,
                                                      path=object_store_uri,
                                                      destination=destination))
        return self

    def object(self, object_store_uri: str, destination: str) -> ImportManager:
        """
        Creates a new import source of type "object" and adds it to the list of sources

        :param object_store_uri: The URI from which to import the object
        :param destination: The destination path for the object relative to the branch
        :return: The ImportManager instance (self) after update, to allow operations chaining
        """
        self._append_source(lakefs_sdk.ImportLocation(type=_OBJECT, path=object_store_uri, destination=destination))
        return self

    def start(self) -> str:
        """
        Start import, reporting back (and storing) a process id

        :return: The import process identifier in lakeFS
        :raise ImportManagerException: if an import process is already in progress
        :raise NotFoundException: if branch or repository do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ValidationError: if path_type is not one of the allowed values
        :raise ServerException: for any other errors
        """
        if self._in_progress:
            raise ImportManagerException("Import in progress")
        if self._import_id is not None:
            raise ImportManagerException("Import Manager can only be used once")

        creation = lakefs_sdk.ImportCreation(paths=self.sources,
                                             commit=lakefs_sdk.CommitCreation(message=self.commit_message,
                                                                              metadata=self.commit_metadata))
        with api_exception_handler():
            res = self._client.sdk_client.import_api.import_start(repository=self._repo_id,
                                                                  branch=self._branch_id,
                                                                  import_creation=creation)
            self._import_id = res.id
            self._in_progress = True

        return self._import_id

    async def _wait_for_completion(self, poll_interval: timedelta) -> lakefs_sdk.ImportStatus:
        while True:
            with api_exception_handler():
                resp = self._client.sdk_client.import_api.import_status(repository=self._repo_id,
                                                                        branch=self._branch_id,
                                                                        id=self._import_id)
            if resp.completed:
                return resp
            if resp.error is not None:
                raise ImportManagerException(f"Import Error: {resp.error.message}")

            await asyncio.sleep(poll_interval.total_seconds())

    def wait(self, poll_interval: Optional[timedelta] = timedelta(seconds=2)) -> ImportStatus:
        """
        Poll a started import task ID, blocking until completion

        :param poll_interval: The interval for polling the import status.
        :return: Import status as returned by the lakeFS server
        :raise ImportManagerException: if no import is in progress
        :raise NotFoundException: if branch, repository or import id do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """
        if self._import_id is None:
            raise ImportManagerException("No import in progress")

        res = asyncio.run(self._wait_for_completion(poll_interval))
        self._in_progress = False
        return ImportStatus(**res.dict())

    def run(self, poll_interval: Optional[timedelta] = None) -> ImportStatus:
        """
        Same as calling start() and then wait()

        :param poll_interval: The interval for polling the import status.
        :return: Import status as returned by the lakeFS server
        :raises: See start(), wait()
        """
        self.start()
        wait_kwargs = {} if poll_interval is None else {"poll_interval": poll_interval}
        return self.wait(**wait_kwargs)

    def cancel(self) -> None:
        """
        Cancel an ongoing import process

        :raise NotFoundException: if branch, repository or import id do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ConflictException: if the import was already completed
        :raise ServerException: for any other errors
        """
        if self._import_id is None:  # Can't cancel on no id
            raise ImportManagerException("No import in progress")

        with api_exception_handler():
            self._client.sdk_client.import_api.import_cancel(repository=self._repo_id,
                                                             branch=self._branch_id,
                                                             id=self._import_id)
            self._in_progress = False

    def status(self) -> ImportStatus:
        """
        Get the current import status

        :return: Import status as returned by the lakeFS server
        :raise ImportManagerException: if no import is in progress
        :raise NotFoundException: if branch, repository or import id do not exist
        :raise NotAuthorizedException: if user is not authorized to perform this operation
        :raise ServerException: for any other errors
        """

        if self._import_id is None:
            raise ImportManagerException("No import in progress")

        with api_exception_handler():
            res = self._client.sdk_client.import_api.import_status(repository=self._repo_id,
                                                                   branch=self._branch_id,
                                                                   id=self._import_id)

            if res.completed:
                self._in_progress = False
            return ImportStatus(**res.dict())
