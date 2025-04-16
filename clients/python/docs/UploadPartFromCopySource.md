# UploadPartFromCopySource

Source of copy, required for type \"copy\"

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**repository** | **str** |  | 
**ref** | **str** |  | 
**path** | **str** |  | 
**range** | **str** | Range of bytes to copy | [optional] 

## Example

```python
from lakefs_sdk.models.upload_part_from_copy_source import UploadPartFromCopySource

# TODO update the JSON string below
json = "{}"
# create an instance of UploadPartFromCopySource from a JSON string
upload_part_from_copy_source_instance = UploadPartFromCopySource.from_json(json)
# print the JSON string representation of the object
print UploadPartFromCopySource.to_json()

# convert the object into a dict
upload_part_from_copy_source_dict = upload_part_from_copy_source_instance.to_dict()
# create an instance of UploadPartFromCopySource from a dict
upload_part_from_copy_source_form_dict = upload_part_from_copy_source.from_dict(upload_part_from_copy_source_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


