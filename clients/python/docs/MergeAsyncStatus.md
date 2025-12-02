# MergeAsyncStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**task_id** | **str** | the id of the async merge task | 
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **datetime** | last time the task status was updated | 
**result** | [**MergeResult**](MergeResult.md) |  | [optional] 
**error** | [**Error**](Error.md) |  | [optional] 
**status_code** | **int** | an http status code that correlates with the underlying error if exists | [optional] 

## Example

```python
from lakefs_sdk.models.merge_async_status import MergeAsyncStatus

# TODO update the JSON string below
json = "{}"
# create an instance of MergeAsyncStatus from a JSON string
merge_async_status_instance = MergeAsyncStatus.from_json(json)
# print the JSON string representation of the object
print MergeAsyncStatus.to_json()

# convert the object into a dict
merge_async_status_dict = merge_async_status_instance.to_dict()
# create an instance of MergeAsyncStatus from a dict
merge_async_status_form_dict = merge_async_status.from_dict(merge_async_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


