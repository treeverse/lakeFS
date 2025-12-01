# CommitStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**task_id** | **str** | the id of the async commit task | 
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **datetime** | last time the task status was updated | 
**result** | [**Commit**](Commit.md) |  | [optional] 
**error** | [**Error**](Error.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.commit_status import CommitStatus

# TODO update the JSON string below
json = "{}"
# create an instance of CommitStatus from a JSON string
commit_status_instance = CommitStatus.from_json(json)
# print the JSON string representation of the object
print CommitStatus.to_json()

# convert the object into a dict
commit_status_dict = commit_status_instance.to_dict()
# create an instance of CommitStatus from a dict
commit_status_form_dict = commit_status.from_dict(commit_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


