# DeleteBranchProtectionRuleRequest


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pattern** | **str** |  | 

## Example

```python
from lakefs_sdk.models.delete_branch_protection_rule_request import DeleteBranchProtectionRuleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DeleteBranchProtectionRuleRequest from a JSON string
delete_branch_protection_rule_request_instance = DeleteBranchProtectionRuleRequest.from_json(json)
# print the JSON string representation of the object
print DeleteBranchProtectionRuleRequest.to_json()

# convert the object into a dict
delete_branch_protection_rule_request_dict = delete_branch_protection_rule_request_instance.to_dict()
# create an instance of DeleteBranchProtectionRuleRequest from a dict
delete_branch_protection_rule_request_form_dict = delete_branch_protection_rule_request.from_dict(delete_branch_protection_rule_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


