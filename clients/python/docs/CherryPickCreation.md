# CherryPickCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ref** | **str** | the commit to cherry-pick, given by a ref | 
**parent_number** | **int** | When cherry-picking a merge commit, the parent number (starting from 1) with which to perform the diff. The default branch is parent 1.  | [optional] 
**commit_overrides** | [**CommitOverrides**](CommitOverrides.md) |  | [optional] 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.cherry_pick_creation import CherryPickCreation

# TODO update the JSON string below
json = "{}"
# create an instance of CherryPickCreation from a JSON string
cherry_pick_creation_instance = CherryPickCreation.from_json(json)
# print the JSON string representation of the object
print CherryPickCreation.to_json()

# convert the object into a dict
cherry_pick_creation_dict = cherry_pick_creation_instance.to_dict()
# create an instance of CherryPickCreation from a dict
cherry_pick_creation_form_dict = cherry_pick_creation.from_dict(cherry_pick_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


