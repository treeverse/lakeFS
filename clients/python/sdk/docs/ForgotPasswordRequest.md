# ForgotPasswordRequest


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email** | **str** |  | 

## Example

```python
from lakefs_sdk.models.forgot_password_request import ForgotPasswordRequest

# TODO update the JSON string below
json = "{}"
# create an instance of ForgotPasswordRequest from a JSON string
forgot_password_request_instance = ForgotPasswordRequest.from_json(json)
# print the JSON string representation of the object
print ForgotPasswordRequest.to_json()

# convert the object into a dict
forgot_password_request_dict = forgot_password_request_instance.to_dict()
# create an instance of ForgotPasswordRequest from a dict
forgot_password_request_form_dict = forgot_password_request.from_dict(forgot_password_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


