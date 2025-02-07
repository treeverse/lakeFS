# CredentialsWithSecret


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_key_id** | **str** |  | 
**secret_access_key** | **str** |  | 
**creation_date** | **int** | Unix Epoch in seconds | 

## Example

```python
from lakefs_sdk.models.credentials_with_secret import CredentialsWithSecret

# TODO update the JSON string below
json = "{}"
# create an instance of CredentialsWithSecret from a JSON string
credentials_with_secret_instance = CredentialsWithSecret.from_json(json)
# print the JSON string representation of the object
print CredentialsWithSecret.to_json()

# convert the object into a dict
credentials_with_secret_dict = credentials_with_secret_instance.to_dict()
# create an instance of CredentialsWithSecret from a dict
credentials_with_secret_form_dict = credentials_with_secret.from_dict(credentials_with_secret_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


