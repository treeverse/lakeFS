# ObjectErrorList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**errors** | [**List[ObjectError]**](ObjectError.md) |  | 

## Example

```python
from lakefs_sdk.models.object_error_list import ObjectErrorList

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectErrorList from a JSON string
object_error_list_instance = ObjectErrorList.from_json(json)
# print the JSON string representation of the object
print(ObjectErrorList.to_json())

# convert the object into a dict
object_error_list_dict = object_error_list_instance.to_dict()
# create an instance of ObjectErrorList from a dict
object_error_list_from_dict = ObjectErrorList.from_dict(object_error_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


