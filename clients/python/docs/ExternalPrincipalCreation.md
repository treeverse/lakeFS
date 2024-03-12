# ExternalPrincipalCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**settings** | **List[Dict[str, str]]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.external_principal_creation import ExternalPrincipalCreation

# TODO update the JSON string below
json = "{}"
# create an instance of ExternalPrincipalCreation from a JSON string
external_principal_creation_instance = ExternalPrincipalCreation.from_json(json)
# print the JSON string representation of the object
print ExternalPrincipalCreation.to_json()

# convert the object into a dict
external_principal_creation_dict = external_principal_creation_instance.to_dict()
# create an instance of ExternalPrincipalCreation from a dict
external_principal_creation_form_dict = external_principal_creation.from_dict(external_principal_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


