# PrepareGarbageCollectionCommitsStatus


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**task_id** | **str** | the id of the task preparing the GC commits | 
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **datetime** | last time the task status was updated | 
**result** | [**GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md) |  | [optional] 
**error** | [**Error**](Error.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.prepare_garbage_collection_commits_status import PrepareGarbageCollectionCommitsStatus

# TODO update the JSON string below
json = "{}"
# create an instance of PrepareGarbageCollectionCommitsStatus from a JSON string
prepare_garbage_collection_commits_status_instance = PrepareGarbageCollectionCommitsStatus.from_json(json)
# print the JSON string representation of the object
print PrepareGarbageCollectionCommitsStatus.to_json()

# convert the object into a dict
prepare_garbage_collection_commits_status_dict = prepare_garbage_collection_commits_status_instance.to_dict()
# create an instance of PrepareGarbageCollectionCommitsStatus from a dict
prepare_garbage_collection_commits_status_form_dict = prepare_garbage_collection_commits_status.from_dict(prepare_garbage_collection_commits_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


