# User


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | A unique identifier for the user. Cannot be edited. | 
**creation_date** | **int** | Unix Epoch in seconds | 
**friendly_name** | **str** | A shorter name for the user than the id. Unlike id it does not identify the user (it might not be unique). Used in some places in the UI.  | [optional] 
**email** | **str** | The email address of the user. If API authentication is enabled, this field is mandatory and will be invited to login. If API authentication is disabled, this field will be ignored. All current APIAuthenticators require the email to be  lowercase and unique, although custom authenticators may not enforce this.  | [optional] 

## Example

```python
from lakefs_sdk.models.user import User

# TODO update the JSON string below
json = "{}"
# create an instance of User from a JSON string
user_instance = User.from_json(json)
# print the JSON string representation of the object
print(User.to_json())

# convert the object into a dict
user_dict = user_instance.to_dict()
# create an instance of User from a dict
user_from_dict = User.from_dict(user_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


