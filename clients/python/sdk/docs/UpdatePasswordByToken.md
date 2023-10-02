# UpdatePasswordByToken


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**token** | **str** | token used for authentication | 
**new_password** | **str** | new password to update | 
**email** | **str** | optional user email to match the token for verification | [optional] 

## Example

```python
from lakefs_sdk.models.update_password_by_token import UpdatePasswordByToken

# TODO update the JSON string below
json = "{}"
# create an instance of UpdatePasswordByToken from a JSON string
update_password_by_token_instance = UpdatePasswordByToken.from_json(json)
# print the JSON string representation of the object
print UpdatePasswordByToken.to_json()

# convert the object into a dict
update_password_by_token_dict = update_password_by_token_instance.to_dict()
# create an instance of UpdatePasswordByToken from a dict
update_password_by_token_form_dict = update_password_by_token.from_dict(update_password_by_token_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


