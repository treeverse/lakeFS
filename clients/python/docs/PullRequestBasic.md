# PullRequestBasic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status** | **str** |  | [optional] 
**title** | **str** |  | [optional] 
**description** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.pull_request_basic import PullRequestBasic

# TODO update the JSON string below
json = "{}"
# create an instance of PullRequestBasic from a JSON string
pull_request_basic_instance = PullRequestBasic.from_json(json)
# print the JSON string representation of the object
print PullRequestBasic.to_json()

# convert the object into a dict
pull_request_basic_dict = pull_request_basic_instance.to_dict()
# create an instance of PullRequestBasic from a dict
pull_request_basic_form_dict = pull_request_basic.from_dict(pull_request_basic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


