# BranchCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**source** | **str** |  | 
**force** | **bool** |  | [optional] [default to False]
**hidden** | **bool** | When set, branch will not show up when listing branches by default. *EXPERIMENTAL* | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.branch_creation import BranchCreation

# TODO update the JSON string below
json = "{}"
# create an instance of BranchCreation from a JSON string
branch_creation_instance = BranchCreation.from_json(json)
# print the JSON string representation of the object
print BranchCreation.to_json()

# convert the object into a dict
branch_creation_dict = branch_creation_instance.to_dict()
# create an instance of BranchCreation from a dict
branch_creation_form_dict = branch_creation.from_dict(branch_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


