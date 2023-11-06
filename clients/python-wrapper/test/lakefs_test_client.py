from typing import Optional, Union

from lakefs_sdk import ObjectStats
from typing_extensions import Annotated

from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.api import ObjectsApi
from pydantic import StrictStr, Field, StrictBool, constr, StrictBytes


class TestLakeFSClient(LakeFSClient):
    def __init__(self, configuration=None, header_name=None, header_value=None, cookie=None, pool_threads=1):
        super().__init__(configuration, header_name, header_value, cookie, pool_threads)


class TestObjectAPI(ObjectsApi):
    def upload_object(self, repository: StrictStr, branch: StrictStr,
                      path: Annotated[StrictStr, Field(..., description="relative to the branch")],
                      storage_class: Annotated[Optional[StrictStr], Field(
                          description="Deprecated, this capability will not be supported in future releases.")] = None,
                      if_none_match: Annotated[Optional[constr(strict=True)], Field(
                          description="Currently supports only \"*\" to allow uploading an object only if one doesn't exist yet. Deprecated, this capability will not be supported in future releases. ")] = None,
                      content: Annotated[Optional[Union[StrictBytes, StrictStr]], Field(
                          description="Only a single file per upload which must be named \\\"content\\\".")] = None,
                      **kwargs) -> ObjectStats:
        return super().upload_object(repository, branch, path, storage_class, if_none_match, content, **kwargs)

    def head_object(self, repository: StrictStr, ref: Annotated[
        StrictStr, Field(..., description="a reference (could be either a branch or a commit ID)")],
                    path: Annotated[StrictStr, Field(..., description="relative to the ref")],
                    range: Annotated[Optional[constr(strict=True)], Field(description="Byte range to retrieve")] = None,
                    **kwargs) -> None:
        super().head_object(repository, ref, path, range, **kwargs)

    def get_object(self, repository: StrictStr, ref: Annotated[
        StrictStr, Field(..., description="a reference (could be either a branch or a commit ID)")],
                   path: Annotated[StrictStr, Field(..., description="relative to the ref")],
                   range: Annotated[Optional[constr(strict=True)], Field(description="Byte range to retrieve")] = None,
                   presign: Optional[StrictBool] = None, **kwargs) -> bytearray:
        return super().get_object(repository, ref, path, range, presign, **kwargs)

# class TestStagingAPI(StagingApi):
#     def link_physical_address(self, repository: StrictStr, branch: StrictStr,
#                               path: Annotated[StrictStr, Field(..., description="relative to the branch")],
#                               staging_metadata: StagingMetadata, **kwargs) -> ObjectStats:
#         return super().link_physical_address(repository, branch, path, staging_metadata, **kwargs)
# 
#     def get_physical_address(self, repository: StrictStr, branch: StrictStr,
#                              path: Annotated[StrictStr, Field(..., description="relative to the branch")],
#                              presign: Optional[StrictBool] = None, **kwargs) -> StagingLocation:
#         return super().get_physical_address(repository, branch, path, presign, **kwargs)
