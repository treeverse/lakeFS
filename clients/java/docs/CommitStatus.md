

# CommitStatus


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**taskId** | **String** | the id of the async commit task |  |
|**completed** | **Boolean** | true if the task has completed (either successfully or with an error) |  |
|**updateTime** | **OffsetDateTime** | last time the task status was updated |  |
|**result** | [**Commit**](Commit.md) |  |  [optional] |
|**error** | [**Error**](Error.md) |  |  [optional] |



