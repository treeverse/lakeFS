# TaskCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the new task | 

## Example

```python
from lakefs_sdk.models.task_creation import TaskCreation

# TODO update the JSON string below
json = "{}"
# create an instance of TaskCreation from a JSON string
task_creation_instance = TaskCreation.from_json(json)
# print the JSON string representation of the object
print TaskCreation.to_json()

# convert the object into a dict
task_creation_dict = task_creation_instance.to_dict()
# create an instance of TaskCreation from a dict
task_creation_form_dict = task_creation.from_dict(task_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


