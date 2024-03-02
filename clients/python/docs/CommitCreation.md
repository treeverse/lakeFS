# CommitCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** |  | 
**metadata** | **Dict[str, str]** |  | [optional] 
**var_date** | **int** | set date to override creation date in the commit (Unix Epoch in seconds) | [optional] 
**allow_empty** | **bool** | sets whether a commit can contain no changes | [optional] [default to False]
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.commit_creation import CommitCreation

# TODO update the JSON string below
json = "{}"
# create an instance of CommitCreation from a JSON string
commit_creation_instance = CommitCreation.from_json(json)
# print the JSON string representation of the object
print CommitCreation.to_json()

# convert the object into a dict
commit_creation_dict = commit_creation_instance.to_dict()
# create an instance of CommitCreation from a dict
commit_creation_form_dict = commit_creation.from_dict(commit_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


