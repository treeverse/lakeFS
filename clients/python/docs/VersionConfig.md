# VersionConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**version** | **str** |  | [optional] 
**latest_version** | **str** |  | [optional] 
**upgrade_recommended** | **bool** |  | [optional] 
**upgrade_url** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.version_config import VersionConfig

# TODO update the JSON string below
json = "{}"
# create an instance of VersionConfig from a JSON string
version_config_instance = VersionConfig.from_json(json)
# print the JSON string representation of the object
print(VersionConfig.to_json())

# convert the object into a dict
version_config_dict = version_config_instance.to_dict()
# create an instance of VersionConfig from a dict
version_config_from_dict = VersionConfig.from_dict(version_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


