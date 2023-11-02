# UserCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | a unique identifier for the user. | 
**invite_user** | **bool** |  | [optional] 

## Example

```python
from lakefs_sdk.models.user_creation import UserCreation

# TODO update the JSON string below
json = "{}"
# create an instance of UserCreation from a JSON string
user_creation_instance = UserCreation.from_json(json)
# print the JSON string representation of the object
print UserCreation.to_json()

# convert the object into a dict
user_creation_dict = user_creation_instance.to_dict()
# create an instance of UserCreation from a dict
user_creation_form_dict = user_creation.from_dict(user_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


