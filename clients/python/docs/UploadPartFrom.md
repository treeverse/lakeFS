# UploadPartFrom


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** | The physical address (of the entire intended object) returned from createPresignMultipartUpload.  | 

## Example

```python
from lakefs_sdk.models.upload_part_from import UploadPartFrom

# TODO update the JSON string below
json = "{}"
# create an instance of UploadPartFrom from a JSON string
upload_part_from_instance = UploadPartFrom.from_json(json)
# print the JSON string representation of the object
print UploadPartFrom.to_json()

# convert the object into a dict
upload_part_from_dict = upload_part_from_instance.to_dict()
# create an instance of UploadPartFrom from a dict
upload_part_from_form_dict = upload_part_from.from_dict(upload_part_from_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


