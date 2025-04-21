# UploadPartCopyFrom


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** | The physical address (of the entire intended object) returned from createPresignMultipartUpload.  | 
**copy_source** | [**CopyPartSource**](CopyPartSource.md) |  | 

## Example

```python
from lakefs_sdk.models.upload_part_copy_from import UploadPartCopyFrom

# TODO update the JSON string below
json = "{}"
# create an instance of UploadPartCopyFrom from a JSON string
upload_part_copy_from_instance = UploadPartCopyFrom.from_json(json)
# print the JSON string representation of the object
print UploadPartCopyFrom.to_json()

# convert the object into a dict
upload_part_copy_from_dict = upload_part_copy_from_instance.to_dict()
# create an instance of UploadPartCopyFrom from a dict
upload_part_copy_from_form_dict = upload_part_copy_from.from_dict(upload_part_copy_from_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


