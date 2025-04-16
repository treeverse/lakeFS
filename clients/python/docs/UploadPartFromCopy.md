# UploadPartFromCopy

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
from lakefs_sdk.models.upload_part_from_copy import UploadPartFromCopy

# TODO update the JSON string below
json = "{}"
# create an instance of UploadPartFromCopy from a JSON string
upload_part_from_copy_instance = UploadPartFromCopy.from_json(json)
# print the JSON string representation of the object
print UploadPartFromCopy.to_json()

# convert the object into a dict
upload_part_from_copy_dict = upload_part_from_copy_instance.to_dict()
# create an instance of UploadPartFromCopy from a dict
upload_part_from_copy_form_dict = upload_part_from_copy.from_dict(upload_part_from_copy_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


