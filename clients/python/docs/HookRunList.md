# HookRunList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[HookRun]**](HookRun.md) |  | 

## Example

```python
from lakefs_sdk.models.hook_run_list import HookRunList

# TODO update the JSON string below
json = "{}"
# create an instance of HookRunList from a JSON string
hook_run_list_instance = HookRunList.from_json(json)
# print the JSON string representation of the object
print(HookRunList.to_json())

# convert the object into a dict
hook_run_list_dict = hook_run_list_instance.to_dict()
# create an instance of HookRunList from a dict
hook_run_list_from_dict = HookRunList.from_dict(hook_run_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


