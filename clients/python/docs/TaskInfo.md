# TaskInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the task | 

## Example

```python
from lakefs_sdk.models.task_info import TaskInfo

# TODO update the JSON string below
json = "{}"
# create an instance of TaskInfo from a JSON string
task_info_instance = TaskInfo.from_json(json)
# print the JSON string representation of the object
print(TaskInfo.to_json())

# convert the object into a dict
task_info_dict = task_info_instance.to_dict()
# create an instance of TaskInfo from a dict
task_info_from_dict = TaskInfo.from_dict(task_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


