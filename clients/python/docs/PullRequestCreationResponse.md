# PullRequestCreationResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the pull request | 

## Example

```python
from lakefs_sdk.models.pull_request_creation_response import PullRequestCreationResponse

# TODO update the JSON string below
json = "{}"
# create an instance of PullRequestCreationResponse from a JSON string
pull_request_creation_response_instance = PullRequestCreationResponse.from_json(json)
# print the JSON string representation of the object
print PullRequestCreationResponse.to_json()

# convert the object into a dict
pull_request_creation_response_dict = pull_request_creation_response_instance.to_dict()
# create an instance of PullRequestCreationResponse from a dict
pull_request_creation_response_form_dict = pull_request_creation_response.from_dict(pull_request_creation_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


