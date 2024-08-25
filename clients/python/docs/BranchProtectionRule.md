# BranchProtectionRule


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pattern** | **str** | fnmatch pattern for the branch name, supporting * and ? wildcards | 

## Example

```python
from lakefs_sdk.models.branch_protection_rule import BranchProtectionRule

# TODO update the JSON string below
json = "{}"
# create an instance of BranchProtectionRule from a JSON string
branch_protection_rule_instance = BranchProtectionRule.from_json(json)
# print the JSON string representation of the object
print(BranchProtectionRule.to_json())

# convert the object into a dict
branch_protection_rule_dict = branch_protection_rule_instance.to_dict()
# create an instance of BranchProtectionRule from a dict
branch_protection_rule_from_dict = BranchProtectionRule.from_dict(branch_protection_rule_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


