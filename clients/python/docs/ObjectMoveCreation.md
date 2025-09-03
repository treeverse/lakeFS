# ObjectMoveCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**src_path** | **str** | path of the moved object relative to the ref | 
**src_ref** | **str** | a reference, if empty uses the provided branch as ref | [optional] 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.object_move_creation import ObjectMoveCreation

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectMoveCreation from a JSON string
object_move_creation_instance = ObjectMoveCreation.from_json(json)
# print the JSON string representation of the object
print ObjectMoveCreation.to_json()

# convert the object into a dict
object_move_creation_dict = object_move_creation_instance.to_dict()
# create an instance of ObjectMoveCreation from a dict
object_move_creation_form_dict = object_move_creation.from_dict(object_move_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


