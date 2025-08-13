# ExternalPrincipalList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[ExternalPrincipal]**](ExternalPrincipal.md) |  | 

## Example

```python
from lakefs_sdk.models.external_principal_list import ExternalPrincipalList

# TODO update the JSON string below
json = "{}"
# create an instance of ExternalPrincipalList from a JSON string
external_principal_list_instance = ExternalPrincipalList.from_json(json)
# print the JSON string representation of the object
print ExternalPrincipalList.to_json()

# convert the object into a dict
external_principal_list_dict = external_principal_list_instance.to_dict()
# create an instance of ExternalPrincipalList from a dict
external_principal_list_form_dict = external_principal_list.from_dict(external_principal_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


