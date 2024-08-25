# ActionRunList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[ActionRun]**](ActionRun.md) |  | 

## Example

```python
from lakefs_sdk.models.action_run_list import ActionRunList

# TODO update the JSON string below
json = "{}"
# create an instance of ActionRunList from a JSON string
action_run_list_instance = ActionRunList.from_json(json)
# print the JSON string representation of the object
print(ActionRunList.to_json())

# convert the object into a dict
action_run_list_dict = action_run_list_instance.to_dict()
# create an instance of ActionRunList from a dict
action_run_list_from_dict = ActionRunList.from_dict(action_run_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


