# ACL


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**permission** | **str** | Permission level to give this ACL.  \&quot;Read\&quot;, \&quot;Write\&quot;, \&quot;Super\&quot; and \&quot;Admin\&quot; are all supported.  | 

## Example

```python
from lakefs_sdk.models.acl import ACL

# TODO update the JSON string below
json = "{}"
# create an instance of ACL from a JSON string
acl_instance = ACL.from_json(json)
# print the JSON string representation of the object
print ACL.to_json()

# convert the object into a dict
acl_dict = acl_instance.to_dict()
# create an instance of ACL from a dict
acl_form_dict = acl.from_dict(acl_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


