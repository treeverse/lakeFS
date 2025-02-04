# PullRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status** | **str** |  | 
**title** | **str** |  | 
**description** | **str** |  | 
**id** | **str** |  | 
**creation_date** | **datetime** |  | 
**author** | **str** |  | 
**source_branch** | **str** |  | 
**destination_branch** | **str** |  | 
**merged_commit_id** | **str** | the commit id of merged PRs | [optional] 
**closed_date** | **datetime** |  | [optional] 

## Example

```python
from lakefs_sdk.models.pull_request import PullRequest

# TODO update the JSON string below
json = "{}"
# create an instance of PullRequest from a JSON string
pull_request_instance = PullRequest.from_json(json)
# print the JSON string representation of the object
print(PullRequest.to_json())

# convert the object into a dict
pull_request_dict = pull_request_instance.to_dict()
# create an instance of PullRequest from a dict
pull_request_from_dict = PullRequest.from_dict(pull_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


