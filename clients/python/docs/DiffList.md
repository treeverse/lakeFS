# DiffList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Diff]**](Diff.md) |  | 

## Example

```python
from lakefs_sdk.models.diff_list import DiffList

# TODO update the JSON string below
json = "{}"
# create an instance of DiffList from a JSON string
diff_list_instance = DiffList.from_json(json)
# print the JSON string representation of the object
print DiffList.to_json()

# convert the object into a dict
diff_list_dict = diff_list_instance.to_dict()
# create an instance of DiffList from a dict
diff_list_form_dict = diff_list.from_dict(diff_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


