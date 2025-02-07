# PullRequestsList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[PullRequest]**](PullRequest.md) |  | 

## Example

```python
from lakefs_sdk.models.pull_requests_list import PullRequestsList

# TODO update the JSON string below
json = "{}"
# create an instance of PullRequestsList from a JSON string
pull_requests_list_instance = PullRequestsList.from_json(json)
# print the JSON string representation of the object
print PullRequestsList.to_json()

# convert the object into a dict
pull_requests_list_dict = pull_requests_list_instance.to_dict()
# create an instance of PullRequestsList from a dict
pull_requests_list_form_dict = pull_requests_list.from_dict(pull_requests_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


