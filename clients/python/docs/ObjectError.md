# ObjectError


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status_code** | **int** | HTTP status code associated for operation on path | 
**message** | **str** | short message explaining status_code | 
**path** | **str** | affected path | [optional] 

## Example

```python
from lakefs_sdk.models.object_error import ObjectError

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectError from a JSON string
object_error_instance = ObjectError.from_json(json)
# print the JSON string representation of the object
print(ObjectError.to_json())

# convert the object into a dict
object_error_dict = object_error_instance.to_dict()
# create an instance of ObjectError from a dict
object_error_from_dict = ObjectError.from_dict(object_error_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


