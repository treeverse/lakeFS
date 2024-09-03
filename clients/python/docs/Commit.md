# Commit


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**parents** | **List[str]** |  | 
**committer** | **str** |  | 
**message** | **str** |  | 
**creation_date** | **int** | Unix Epoch in seconds | 
**meta_range_id** | **str** |  | 
**metadata** | **Dict[str, str]** |  | [optional] 
**generation** | **int** |  | [optional] 
**version** | **int** |  | [optional] 

## Example

```python
from lakefs_sdk.models.commit import Commit

# TODO update the JSON string below
json = "{}"
# create an instance of Commit from a JSON string
commit_instance = Commit.from_json(json)
# print the JSON string representation of the object
print(Commit.to_json())

# convert the object into a dict
commit_dict = commit_instance.to_dict()
# create an instance of Commit from a dict
commit_from_dict = Commit.from_dict(commit_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


