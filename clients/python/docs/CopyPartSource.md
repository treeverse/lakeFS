# CopyPartSource


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**repository** | **str** |  | 
**ref** | **str** |  | 
**path** | **str** |  | 
**range** | **str** | Range of bytes to copy | [optional] 

## Example

```python
from lakefs_sdk.models.copy_part_source import CopyPartSource

# TODO update the JSON string below
json = "{}"
# create an instance of CopyPartSource from a JSON string
copy_part_source_instance = CopyPartSource.from_json(json)
# print the JSON string representation of the object
print CopyPartSource.to_json()

# convert the object into a dict
copy_part_source_dict = copy_part_source_instance.to_dict()
# create an instance of CopyPartSource from a dict
copy_part_source_form_dict = copy_part_source.from_dict(copy_part_source_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


