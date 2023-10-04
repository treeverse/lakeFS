# RefsRestoreTask


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the task | 
**refs** | [**RefsDump**](RefsDump.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.refs_restore_task import RefsRestoreTask

# TODO update the JSON string below
json = "{}"
# create an instance of RefsRestoreTask from a JSON string
refs_restore_task_instance = RefsRestoreTask.from_json(json)
# print the JSON string representation of the object
print RefsRestoreTask.to_json()

# convert the object into a dict
refs_restore_task_dict = refs_restore_task_instance.to_dict()
# create an instance of RefsRestoreTask from a dict
refs_restore_task_form_dict = refs_restore_task.from_dict(refs_restore_task_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


