# SetupState


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**state** | **str** |  | [optional] 
**comm_prefs_missing** | **bool** | true if the comm prefs are missing. | [optional] 
**login_config** | [**LoginConfig**](LoginConfig.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.setup_state import SetupState

# TODO update the JSON string below
json = "{}"
# create an instance of SetupState from a JSON string
setup_state_instance = SetupState.from_json(json)
# print the JSON string representation of the object
print(SetupState.to_json())

# convert the object into a dict
setup_state_dict = setup_state_instance.to_dict()
# create an instance of SetupState from a dict
setup_state_from_dict = SetupState.from_dict(setup_state_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


