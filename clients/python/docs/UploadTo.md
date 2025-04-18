# UploadTo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**presigned_url** | **str** |  | 

## Example

```python
from lakefs_sdk.models.upload_to import UploadTo

# TODO update the JSON string below
json = "{}"
# create an instance of UploadTo from a JSON string
upload_to_instance = UploadTo.from_json(json)
# print the JSON string representation of the object
print UploadTo.to_json()

# convert the object into a dict
upload_to_dict = upload_to_instance.to_dict()
# create an instance of UploadTo from a dict
upload_to_form_dict = upload_to.from_dict(upload_to_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


