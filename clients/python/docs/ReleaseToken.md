# ReleaseToken


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**token** | **str** | Login request token to release | 

## Example

```python
from lakefs_sdk.models.release_token import ReleaseToken

# TODO update the JSON string below
json = "{}"
# create an instance of ReleaseToken from a JSON string
release_token_instance = ReleaseToken.from_json(json)
# print the JSON string representation of the object
print ReleaseToken.to_json()

# convert the object into a dict
release_token_dict = release_token_instance.to_dict()
# create an instance of ReleaseToken from a dict
release_token_form_dict = release_token.from_dict(release_token_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


