# StagingMetadata

information about uploaded object

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**staging** | [**StagingLocation**](StagingLocation.md) |  | 
**checksum** | **str** | unique identifier of object content on backing store (typically ETag) | 
**size_bytes** | **int** |  | 
**user_metadata** | **Dict[str, str]** |  | [optional] 
**content_type** | **str** | Object media type | [optional] 
**mtime** | **int** | Unix Epoch in seconds.  May be ignored by server. | [optional] 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.staging_metadata import StagingMetadata

# TODO update the JSON string below
json = "{}"
# create an instance of StagingMetadata from a JSON string
staging_metadata_instance = StagingMetadata.from_json(json)
# print the JSON string representation of the object
print StagingMetadata.to_json()

# convert the object into a dict
staging_metadata_dict = staging_metadata_instance.to_dict()
# create an instance of StagingMetadata from a dict
staging_metadata_form_dict = staging_metadata.from_dict(staging_metadata_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


