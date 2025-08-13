# CurrentUser


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**user** | [**User**](User.md) |  | 

## Example

```python
from lakefs_sdk.models.current_user import CurrentUser

# TODO update the JSON string below
json = "{}"
# create an instance of CurrentUser from a JSON string
current_user_instance = CurrentUser.from_json(json)
# print the JSON string representation of the object
print CurrentUser.to_json()

# convert the object into a dict
current_user_dict = current_user_instance.to_dict()
# create an instance of CurrentUser from a dict
current_user_form_dict = current_user.from_dict(current_user_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


