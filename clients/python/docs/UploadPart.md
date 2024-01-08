# UploadPart


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**part_number** | **int** |  | 
**etag** | **str** |  | 

## Example

```python
from lakefs_sdk.models.upload_part import UploadPart

# TODO update the JSON string below
json = "{}"
# create an instance of UploadPart from a JSON string
upload_part_instance = UploadPart.from_json(json)
# print the JSON string representation of the object
print UploadPart.to_json()

# convert the object into a dict
upload_part_dict = upload_part_instance.to_dict()
# create an instance of UploadPart from a dict
upload_part_form_dict = upload_part.from_dict(upload_part_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


