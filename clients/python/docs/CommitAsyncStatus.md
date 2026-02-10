# CommitAsyncStatus


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**task_id** | **str** | the id of the async task | 
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **datetime** | last time the task status was updated | 
**error** | [**Error**](Error.md) |  | [optional] 
**status_code** | **int** | an http status code that correlates with the underlying error if exists | [optional] 
**result** | [**Commit**](Commit.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.commit_async_status import CommitAsyncStatus

# TODO update the JSON string below
json = "{}"
# create an instance of CommitAsyncStatus from a JSON string
commit_async_status_instance = CommitAsyncStatus.from_json(json)
# print the JSON string representation of the object
print CommitAsyncStatus.to_json()

# convert the object into a dict
commit_async_status_dict = commit_async_status_instance.to_dict()
# create an instance of CommitAsyncStatus from a dict
commit_async_status_form_dict = commit_async_status.from_dict(commit_async_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


