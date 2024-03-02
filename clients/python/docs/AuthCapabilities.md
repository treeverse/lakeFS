# AuthCapabilities


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**invite_user** | **bool** |  | [optional] 
**forgot_password** | **bool** |  | [optional] 

## Example

```python
from lakefs_sdk.models.auth_capabilities import AuthCapabilities

# TODO update the JSON string below
json = "{}"
# create an instance of AuthCapabilities from a JSON string
auth_capabilities_instance = AuthCapabilities.from_json(json)
# print the JSON string representation of the object
print AuthCapabilities.to_json()

# convert the object into a dict
auth_capabilities_dict = auth_capabilities_instance.to_dict()
# create an instance of AuthCapabilities from a dict
auth_capabilities_form_dict = auth_capabilities.from_dict(auth_capabilities_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


