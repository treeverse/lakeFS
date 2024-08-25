# CommitList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Commit]**](Commit.md) |  | 

## Example

```python
from lakefs_sdk.models.commit_list import CommitList

# TODO update the JSON string below
json = "{}"
# create an instance of CommitList from a JSON string
commit_list_instance = CommitList.from_json(json)
# print the JSON string representation of the object
print(CommitList.to_json())

# convert the object into a dict
commit_list_dict = commit_list_instance.to_dict()
# create an instance of CommitList from a dict
commit_list_from_dict = CommitList.from_dict(commit_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


