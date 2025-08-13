# GarbageCollectionRules


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**default_retention_days** | **int** |  | 
**branches** | [**List[GarbageCollectionRule]**](GarbageCollectionRule.md) |  | 

## Example

```python
from lakefs_sdk.models.garbage_collection_rules import GarbageCollectionRules

# TODO update the JSON string below
json = "{}"
# create an instance of GarbageCollectionRules from a JSON string
garbage_collection_rules_instance = GarbageCollectionRules.from_json(json)
# print the JSON string representation of the object
print GarbageCollectionRules.to_json()

# convert the object into a dict
garbage_collection_rules_dict = garbage_collection_rules_instance.to_dict()
# create an instance of GarbageCollectionRules from a dict
garbage_collection_rules_form_dict = garbage_collection_rules.from_dict(garbage_collection_rules_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


