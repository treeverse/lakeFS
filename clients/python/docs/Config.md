# Config


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**version_config** | [**VersionConfig**](VersionConfig.md) |  | [optional] 
**storage_config** | [**StorageConfig**](StorageConfig.md) |  | [optional] 
**storage_config_list** | [**List[StorageConfig]**](StorageConfig.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.config import Config

# TODO update the JSON string below
json = "{}"
# create an instance of Config from a JSON string
config_instance = Config.from_json(json)
# print the JSON string representation of the object
print Config.to_json()

# convert the object into a dict
config_dict = config_instance.to_dict()
# create an instance of Config from a dict
config_form_dict = config.from_dict(config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


