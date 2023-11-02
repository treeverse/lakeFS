# Diff


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**path** | **str** |  | 
**path_type** | **str** |  | 
**size_bytes** | **int** | represents the size of the added/changed/deleted entry | [optional] 

## Example

```python
from lakefs_sdk.models.diff import Diff

# TODO update the JSON string below
json = "{}"
# create an instance of Diff from a JSON string
diff_instance = Diff.from_json(json)
# print the JSON string representation of the object
print Diff.to_json()

# convert the object into a dict
diff_dict = diff_instance.to_dict()
# create an instance of Diff from a dict
diff_form_dict = diff.from_dict(diff_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


