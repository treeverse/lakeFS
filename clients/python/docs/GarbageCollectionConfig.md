# GarbageCollectionConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**grace_period** | **int** | Duration in seconds. Objects created in the recent grace_period will not be collected. | [optional] 

## Example

```python
from lakefs_sdk.models.garbage_collection_config import GarbageCollectionConfig

# TODO update the JSON string below
json = "{}"
# create an instance of GarbageCollectionConfig from a JSON string
garbage_collection_config_instance = GarbageCollectionConfig.from_json(json)
# print the JSON string representation of the object
print GarbageCollectionConfig.to_json()

# convert the object into a dict
garbage_collection_config_dict = garbage_collection_config_instance.to_dict()
# create an instance of GarbageCollectionConfig from a dict
garbage_collection_config_form_dict = garbage_collection_config.from_dict(garbage_collection_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


