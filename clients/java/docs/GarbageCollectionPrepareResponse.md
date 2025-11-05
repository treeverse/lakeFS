

# GarbageCollectionPrepareResponse


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**runId** | **String** | a unique identifier generated for this GC job |  |
|**gcCommitsLocation** | **String** | location of the resulting commits csv table (partitioned by run_id) |  [optional] |
|**gcAddressesLocation** | **String** | location to use for expired addresses parquet table (partitioned by run_id) |  [optional] |
|**gcCommitsPresignedUrl** | **String** | a presigned url to download the commits csv |  [optional] |
|**completed** | **Boolean** | true if the task has completed (either successfully or with an error) |  |
|**updateTime** | **OffsetDateTime** | last time the task status was updated |  |
|**error** | [**Error**](Error.md) |  |  [optional] |



