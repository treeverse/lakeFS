# OtfDiffList


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**diff_type** | **str** |  | [optional] 
**results** | [**List[OtfDiffEntry]**](OtfDiffEntry.md) |  | 

## Example

```python
from lakefs_sdk.models.otf_diff_list import OtfDiffList

# TODO update the JSON string below
json = "{}"
# create an instance of OtfDiffList from a JSON string
otf_diff_list_instance = OtfDiffList.from_json(json)
# print the JSON string representation of the object
print OtfDiffList.to_json()

# convert the object into a dict
otf_diff_list_dict = otf_diff_list_instance.to_dict()
# create an instance of OtfDiffList from a dict
otf_diff_list_form_dict = otf_diff_list.from_dict(otf_diff_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


