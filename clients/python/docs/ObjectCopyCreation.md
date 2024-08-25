# ObjectCopyCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**src_path** | **str** | path of the copied object relative to the ref | 
**src_ref** | **str** | a reference, if empty uses the provided branch as ref | [optional] 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.object_copy_creation import ObjectCopyCreation

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectCopyCreation from a JSON string
object_copy_creation_instance = ObjectCopyCreation.from_json(json)
# print the JSON string representation of the object
print(ObjectCopyCreation.to_json())

# convert the object into a dict
object_copy_creation_dict = object_copy_creation_instance.to_dict()
# create an instance of ObjectCopyCreation from a dict
object_copy_creation_from_dict = ObjectCopyCreation.from_dict(object_copy_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


