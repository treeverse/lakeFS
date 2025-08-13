# ExternalLoginInformation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**token_expiration_duration** | **int** |  | [optional] 
**identity_request** | **object** |  | 

## Example

```python
from lakefs_sdk.models.external_login_information import ExternalLoginInformation

# TODO update the JSON string below
json = "{}"
# create an instance of ExternalLoginInformation from a JSON string
external_login_information_instance = ExternalLoginInformation.from_json(json)
# print the JSON string representation of the object
print ExternalLoginInformation.to_json()

# convert the object into a dict
external_login_information_dict = external_login_information_instance.to_dict()
# create an instance of ExternalLoginInformation from a dict
external_login_information_form_dict = external_login_information.from_dict(external_login_information_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


