# ResetCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**operation** | **str** | The kind of reset operation to perform.  If \&quot;staged\&quot;, uncommitted objects according to type.  If \&quot;hard\&quot;, branch must contain no uncommitted objects, and will be reset to refer to ref.  | [optional] [default to 'staged']
**type** | **str** | Only allowed for operation&#x3D;\&quot;staged\&quot;.  Specifies what to reset according to path.  | 
**ref** | **str** | Only allowed for operation&#x3D;\&quot;hard\&quot;.  Branch will be reset to this ref.  | [optional] 
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


