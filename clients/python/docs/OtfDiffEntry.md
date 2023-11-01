# OtfDiffEntry


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**timestamp** | **int** |  | 
**operation** | **str** |  | 
**operation_content** | **object** | free form content describing the returned operation diff | 
**operation_type** | **str** | the operation category (CUD) | 

## Example

```python
from lakefs_sdk.models.otf_diff_entry import OtfDiffEntry

# TODO update the JSON string below
json = "{}"
# create an instance of OtfDiffEntry from a JSON string
otf_diff_entry_instance = OtfDiffEntry.from_json(json)
# print the JSON string representation of the object
print OtfDiffEntry.to_json()

# convert the object into a dict
otf_diff_entry_dict = otf_diff_entry_instance.to_dict()
# create an instance of OtfDiffEntry from a dict
otf_diff_entry_form_dict = otf_diff_entry.from_dict(otf_diff_entry_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


