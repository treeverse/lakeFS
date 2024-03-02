# FindMergeBaseResult


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_commit_id** | **str** | The commit ID of the merge source | 
**destination_commit_id** | **str** | The commit ID of the merge destination | 
**base_commit_id** | **str** | The commit ID of the merge base | 

## Example

```python
from lakefs_sdk.models.find_merge_base_result import FindMergeBaseResult

# TODO update the JSON string below
json = "{}"
# create an instance of FindMergeBaseResult from a JSON string
find_merge_base_result_instance = FindMergeBaseResult.from_json(json)
# print the JSON string representation of the object
print FindMergeBaseResult.to_json()

# convert the object into a dict
find_merge_base_result_dict = find_merge_base_result_instance.to_dict()
# create an instance of FindMergeBaseResult from a dict
find_merge_base_result_form_dict = find_merge_base_result.from_dict(find_merge_base_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


