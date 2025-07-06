# ObjectMetadata


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** |  | 
**checksum** | **str** |  | 
**mtime** | **int** | Unix Epoch in seconds | 
**content_type** | **str** | Object media type | 
**user_metadata** | **Dict[str, str]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.object_metadata import ObjectMetadata

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectMetadata from a JSON string
object_metadata_instance = ObjectMetadata.from_json(json)
# print the JSON string representation of the object
print ObjectMetadata.to_json()

# convert the object into a dict
object_metadata_dict = object_metadata_instance.to_dict()
# create an instance of ObjectMetadata from a dict
object_metadata_form_dict = object_metadata.from_dict(object_metadata_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


