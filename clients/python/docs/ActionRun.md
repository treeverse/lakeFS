# ActionRun


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**run_id** | **str** |  | 
**branch** | **str** |  | 
**start_time** | **datetime** |  | 
**end_time** | **datetime** |  | [optional] 
**event_type** | **str** |  | 
**status** | **str** |  | 
**commit_id** | **str** |  | 

## Example

```python
from lakefs_sdk.models.action_run import ActionRun

# TODO update the JSON string below
json = "{}"
# create an instance of ActionRun from a JSON string
action_run_instance = ActionRun.from_json(json)
# print the JSON string representation of the object
print(ActionRun.to_json())

# convert the object into a dict
action_run_dict = action_run_instance.to_dict()
# create an instance of ActionRun from a dict
action_run_from_dict = ActionRun.from_dict(action_run_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


