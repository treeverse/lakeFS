# PolicyList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Policy]**](Policy.md) |  | 

## Example

```python
from lakefs_sdk.models.policy_list import PolicyList

# TODO update the JSON string below
json = "{}"
# create an instance of PolicyList from a JSON string
policy_list_instance = PolicyList.from_json(json)
# print the JSON string representation of the object
print PolicyList.to_json()

# convert the object into a dict
policy_list_dict = policy_list_instance.to_dict()
# create an instance of PolicyList from a dict
policy_list_form_dict = policy_list.from_dict(policy_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


