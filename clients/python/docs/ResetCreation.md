# ResetCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | What to reset according to path. | 
**path** | **str** |  | [optional] 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.reset_creation import ResetCreation

# TODO update the JSON string below
json = "{}"
# create an instance of ResetCreation from a JSON string
reset_creation_instance = ResetCreation.from_json(json)
# print the JSON string representation of the object
print ResetCreation.to_json()

# convert the object into a dict
reset_creation_dict = reset_creation_instance.to_dict()
# create an instance of ResetCreation from a dict
reset_creation_form_dict = reset_creation.from_dict(reset_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


