# CommPrefsInput


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email** | **str** | the provided email | [optional] 
**feature_updates** | **bool** | was \&quot;feature updates\&quot; checked | 
**security_updates** | **bool** | was \&quot;security updates\&quot; checked | 

## Example

```python
from lakefs_sdk.models.comm_prefs_input import CommPrefsInput

# TODO update the JSON string below
json = "{}"
# create an instance of CommPrefsInput from a JSON string
comm_prefs_input_instance = CommPrefsInput.from_json(json)
# print the JSON string representation of the object
print CommPrefsInput.to_json()

# convert the object into a dict
comm_prefs_input_dict = comm_prefs_input_instance.to_dict()
# create an instance of CommPrefsInput from a dict
comm_prefs_input_form_dict = comm_prefs_input.from_dict(comm_prefs_input_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


