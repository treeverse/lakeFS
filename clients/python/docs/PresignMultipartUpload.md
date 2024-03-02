# PresignMultipartUpload


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**upload_id** | **str** |  | 
**physical_address** | **str** |  | 
**presigned_urls** | **List[str]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.presign_multipart_upload import PresignMultipartUpload

# TODO update the JSON string below
json = "{}"
# create an instance of PresignMultipartUpload from a JSON string
presign_multipart_upload_instance = PresignMultipartUpload.from_json(json)
# print the JSON string representation of the object
print PresignMultipartUpload.to_json()

# convert the object into a dict
presign_multipart_upload_dict = presign_multipart_upload_instance.to_dict()
# create an instance of PresignMultipartUpload from a dict
presign_multipart_upload_form_dict = presign_multipart_upload.from_dict(presign_multipart_upload_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


