# RevertCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ref** | **str** | the commit to revert, given by a ref | 
**commit_overrides** | [**CommitOverrides**](CommitOverrides.md) |  | [optional] 
**parent_number** | **int** | when reverting a merge commit, the parent number (starting from 1) relative to which to perform the revert. | 
**force** | **bool** |  | [optional] [default to False]
**allow_empty** | **bool** | allow empty commit (revert without changes) | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.revert_creation import RevertCreation

# TODO update the JSON string below
json = "{}"
# create an instance of RevertCreation from a JSON string
revert_creation_instance = RevertCreation.from_json(json)
# print the JSON string representation of the object
print RevertCreation.to_json()

# convert the object into a dict
revert_creation_dict = revert_creation_instance.to_dict()
# create an instance of RevertCreation from a dict
revert_creation_form_dict = revert_creation.from_dict(revert_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


