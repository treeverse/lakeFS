# CapabilitiesConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**async_ops** | **bool** | are async operations enabled in server. *EXPERIMENTAL* | [optional] 

## Example

```python
from lakefs_sdk.models.capabilities_config import CapabilitiesConfig

# TODO update the JSON string below
json = "{}"
# create an instance of CapabilitiesConfig from a JSON string
capabilities_config_instance = CapabilitiesConfig.from_json(json)
# print the JSON string representation of the object
print CapabilitiesConfig.to_json()

# convert the object into a dict
capabilities_config_dict = capabilities_config_instance.to_dict()
# create an instance of CapabilitiesConfig from a dict
capabilities_config_form_dict = capabilities_config.from_dict(capabilities_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


