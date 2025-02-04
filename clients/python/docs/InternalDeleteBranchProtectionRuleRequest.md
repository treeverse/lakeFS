# InternalDeleteBranchProtectionRuleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pattern** | **str** |  | 

## Example

```python
from lakefs_sdk.models.internal_delete_branch_protection_rule_request import InternalDeleteBranchProtectionRuleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of InternalDeleteBranchProtectionRuleRequest from a JSON string
internal_delete_branch_protection_rule_request_instance = InternalDeleteBranchProtectionRuleRequest.from_json(json)
# print the JSON string representation of the object
print(InternalDeleteBranchProtectionRuleRequest.to_json())

# convert the object into a dict
internal_delete_branch_protection_rule_request_dict = internal_delete_branch_protection_rule_request_instance.to_dict()
# create an instance of InternalDeleteBranchProtectionRuleRequest from a dict
internal_delete_branch_protection_rule_request_from_dict = InternalDeleteBranchProtectionRuleRequest.from_dict(internal_delete_branch_protection_rule_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


