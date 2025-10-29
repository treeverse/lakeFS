# AuthenticationToken


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**token** | **str** | a JWT token that could be used to authenticate requests | 
**token_expiration** | **int** | Unix Epoch in seconds | [optional] 

## Example

```python
from lakefs_sdk.models.authentication_token import AuthenticationToken

# TODO update the JSON string below
json = "{}"
# create an instance of AuthenticationToken from a JSON string
authentication_token_instance = AuthenticationToken.from_json(json)
# print the JSON string representation of the object
print AuthenticationToken.to_json()

# convert the object into a dict
authentication_token_dict = authentication_token_instance.to_dict()
# create an instance of AuthenticationToken from a dict
authentication_token_form_dict = authentication_token.from_dict(authentication_token_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


