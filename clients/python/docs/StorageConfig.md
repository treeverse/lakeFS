# StorageConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**blockstore_type** | **str** |  | 
**blockstore_namespace_example** | **str** |  | 
**blockstore_namespace_validity_regex** | **str** |  | 
**default_namespace_prefix** | **str** |  | [optional] 
**pre_sign_support** | **bool** |  | 
**pre_sign_support_ui** | **bool** |  | 
**import_support** | **bool** |  | 
**import_validity_regex** | **str** |  | 
**pre_sign_multipart_upload** | **bool** |  | [optional] 

## Example

```python
from lakefs_sdk.models.storage_config import StorageConfig

# TODO update the JSON string below
json = "{}"
# create an instance of StorageConfig from a JSON string
storage_config_instance = StorageConfig.from_json(json)
# print the JSON string representation of the object
print(StorageConfig.to_json())

# convert the object into a dict
storage_config_dict = storage_config_instance.to_dict()
# create an instance of StorageConfig from a dict
storage_config_from_dict = StorageConfig.from_dict(storage_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


