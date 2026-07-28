# CompletePresignMultipartUpload


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** |  | 
**parts** | [**List[UploadPart]**](UploadPart.md) | List of uploaded parts, should be ordered by ascending part number | 
**user_metadata** | **Dict[str, str]** |  | [optional] 
**content_type** | **str** | Object media type | [optional] 
**checksum_algorithm** | [**ChecksumAlgorithm**](ChecksumAlgorithm.md) |  | [optional] 
**checksum_type** | [**ChecksumType**](ChecksumType.md) |  | [optional] 
**checksum** | **str** | Base64-encoded big-endian full-object checksum of the entire object content, computed with checksum_algorithm (S3 encoding convention). Requires checksum_algorithm. lakeFS compares the storage-computed full-object checksum of the assembled object against this value and fails the completion on mismatch; a successful completion means the checksum was validated.  | [optional] 
**mpu_object_size** | **int** | Expected total size in bytes of the assembled object, validated by the storage on completion. May be supplied with or without a checksum.  | [optional] 

## Example

```python
from lakefs_sdk.models.complete_presign_multipart_upload import CompletePresignMultipartUpload

# TODO update the JSON string below
json = "{}"
# create an instance of CompletePresignMultipartUpload from a JSON string
complete_presign_multipart_upload_instance = CompletePresignMultipartUpload.from_json(json)
# print the JSON string representation of the object
print CompletePresignMultipartUpload.to_json()

# convert the object into a dict
complete_presign_multipart_upload_dict = complete_presign_multipart_upload_instance.to_dict()
# create an instance of CompletePresignMultipartUpload from a dict
complete_presign_multipart_upload_form_dict = complete_presign_multipart_upload.from_dict(complete_presign_multipart_upload_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


