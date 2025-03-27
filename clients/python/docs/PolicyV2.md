# PolicyV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**creation_date** | **int** | Unix Epoch in seconds | [optional] 
**statement** | [**List[StatementV2]**](StatementV2.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.policy_v2 import PolicyV2

# TODO update the JSON string below
json = "{}"
# create an instance of PolicyV2 from a JSON string
policy_v2_instance = PolicyV2.from_json(json)
# print the JSON string representation of the object
print PolicyV2.to_json()

# convert the object into a dict
policy_v2_dict = policy_v2_instance.to_dict()
# create an instance of PolicyV2 from a dict
policy_v2_form_dict = policy_v2.from_dict(policy_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


