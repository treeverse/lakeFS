# ObjectStageCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** |  | 
**checksum** | **str** |  | 
**size_bytes** | **int** |  | 
**mtime** | **int** | Unix Epoch in seconds | [optional] 
**metadata** | **Dict[str, str]** |  | [optional] 
**content_type** | **str** | Object media type | [optional] 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.object_stage_creation import ObjectStageCreation

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectStageCreation from a JSON string
object_stage_creation_instance = ObjectStageCreation.from_json(json)
# print the JSON string representation of the object
print(ObjectStageCreation.to_json())

# convert the object into a dict
object_stage_creation_dict = object_stage_creation_instance.to_dict()
# create an instance of ObjectStageCreation from a dict
object_stage_creation_from_dict = ObjectStageCreation.from_dict(object_stage_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


