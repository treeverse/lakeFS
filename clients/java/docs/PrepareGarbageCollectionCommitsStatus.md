

# PrepareGarbageCollectionCommitsStatus


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**taskId** | **String** | the id of the task preparing the GC commits |  |
|**completed** | **Boolean** | true if the task has completed (either successfully or with an error) |  |
|**updateTime** | **OffsetDateTime** | last time the task status was updated |  |
|**result** | [**GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md) |  |  [optional] |
|**error** | [**Error**](Error.md) |  |  [optional] |



