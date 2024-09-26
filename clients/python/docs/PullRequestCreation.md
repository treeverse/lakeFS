# PullRequestCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**title** | **str** |  | 
**description** | **str** |  | [optional] 
**source_branch** | **str** |  | 
**destination_branch** | **str** |  | 

## Example

```python
from lakefs_sdk.models.pull_request_creation import PullRequestCreation

# TODO update the JSON string below
json = "{}"
# create an instance of PullRequestCreation from a JSON string
pull_request_creation_instance = PullRequestCreation.from_json(json)
# print the JSON string representation of the object
print PullRequestCreation.to_json()

# convert the object into a dict
pull_request_creation_dict = pull_request_creation_instance.to_dict()
# create an instance of PullRequestCreation from a dict
pull_request_creation_form_dict = pull_request_creation.from_dict(pull_request_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


