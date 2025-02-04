# Setup


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**username** | **str** | an identifier for the user (e.g. jane.doe) | 
**key** | [**AccessKeyCredentials**](AccessKeyCredentials.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.setup import Setup

# TODO update the JSON string below
json = "{}"
# create an instance of Setup from a JSON string
setup_instance = Setup.from_json(json)
# print the JSON string representation of the object
print(Setup.to_json())

# convert the object into a dict
setup_dict = setup_instance.to_dict()
# create an instance of Setup from a dict
setup_from_dict = Setup.from_dict(setup_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


