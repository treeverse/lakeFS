# ErrorNoACL


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** | short message explaining the error | 
**no_acl** | **bool** | true if the group exists but has no ACL | [optional] 

## Example

```python
from lakefs_sdk.models.error_no_acl import ErrorNoACL

# TODO update the JSON string below
json = "{}"
# create an instance of ErrorNoACL from a JSON string
error_no_acl_instance = ErrorNoACL.from_json(json)
# print the JSON string representation of the object
print(ErrorNoACL.to_json())

# convert the object into a dict
error_no_acl_dict = error_no_acl_instance.to_dict()
# create an instance of ErrorNoACL from a dict
error_no_acl_from_dict = ErrorNoACL.from_dict(error_no_acl_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


