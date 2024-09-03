# HookRun


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**hook_run_id** | **str** |  | 
**action** | **str** |  | 
**hook_id** | **str** |  | 
**start_time** | **datetime** |  | 
**end_time** | **datetime** |  | [optional] 
**status** | **str** |  | 

## Example

```python
from lakefs_sdk.models.hook_run import HookRun

# TODO update the JSON string below
json = "{}"
# create an instance of HookRun from a JSON string
hook_run_instance = HookRun.from_json(json)
# print the JSON string representation of the object
print(HookRun.to_json())

# convert the object into a dict
hook_run_dict = hook_run_instance.to_dict()
# create an instance of HookRun from a dict
hook_run_from_dict = HookRun.from_dict(hook_run_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


