# ExternalPrincipal


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | A unique identifier for the external principal i.e aws:sts::123:assumed-role/role-name | 
**user_id** | **str** | lakeFS user ID to associate with an external principal.  | 
**settings** | **List[Dict[str, str]]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.external_principal import ExternalPrincipal

# TODO update the JSON string below
json = "{}"
# create an instance of ExternalPrincipal from a JSON string
external_principal_instance = ExternalPrincipal.from_json(json)
# print the JSON string representation of the object
print ExternalPrincipal.to_json()

# convert the object into a dict
external_principal_dict = external_principal_instance.to_dict()
# create an instance of ExternalPrincipal from a dict
external_principal_form_dict = external_principal.from_dict(external_principal_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


