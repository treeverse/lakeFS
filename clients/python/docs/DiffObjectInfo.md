# DiffObjectInfo


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
from lakefs_sdk.models.diff_object_info import DiffObjectInfo

# TODO update the JSON string below
json = "{}"
# create an instance of DiffObjectInfo from a JSON string
diff_object_info_instance = DiffObjectInfo.from_json(json)
# print the JSON string representation of the object
print DiffObjectInfo.to_json()

# convert the object into a dict
diff_object_info_dict = diff_object_info_instance.to_dict()
# create an instance of DiffObjectInfo from a dict
diff_object_info_form_dict = diff_object_info.from_dict(diff_object_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


