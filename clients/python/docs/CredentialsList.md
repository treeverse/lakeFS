# CredentialsList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Credentials]**](Credentials.md) |  | 

## Example

```python
from lakefs_sdk.models.credentials_list import CredentialsList

# TODO update the JSON string below
json = "{}"
# create an instance of CredentialsList from a JSON string
credentials_list_instance = CredentialsList.from_json(json)
# print the JSON string representation of the object
print CredentialsList.to_json()

# convert the object into a dict
credentials_list_dict = credentials_list_instance.to_dict()
# create an instance of CredentialsList from a dict
credentials_list_form_dict = credentials_list.from_dict(credentials_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


