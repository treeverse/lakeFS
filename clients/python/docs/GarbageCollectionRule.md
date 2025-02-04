# GarbageCollectionRule


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**branch_id** | **str** |  | 
**retention_days** | **int** |  | 

## Example

```python
from lakefs_sdk.models.garbage_collection_rule import GarbageCollectionRule

# TODO update the JSON string below
json = "{}"
# create an instance of GarbageCollectionRule from a JSON string
garbage_collection_rule_instance = GarbageCollectionRule.from_json(json)
# print the JSON string representation of the object
print(GarbageCollectionRule.to_json())

# convert the object into a dict
garbage_collection_rule_dict = garbage_collection_rule_instance.to_dict()
# create an instance of GarbageCollectionRule from a dict
garbage_collection_rule_from_dict = GarbageCollectionRule.from_dict(garbage_collection_rule_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


