# JWTLoginInformation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**token** | **str** | A JWT issued by the configured external IdP. | 

## Example

```python
from lakefs_sdk.models.jwt_login_information import JWTLoginInformation

# TODO update the JSON string below
json = "{}"
# create an instance of JWTLoginInformation from a JSON string
jwt_login_information_instance = JWTLoginInformation.from_json(json)
# print the JSON string representation of the object
print JWTLoginInformation.to_json()

# convert the object into a dict
jwt_login_information_dict = jwt_login_information_instance.to_dict()
# create an instance of JWTLoginInformation from a dict
jwt_login_information_form_dict = jwt_login_information.from_dict(jwt_login_information_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


