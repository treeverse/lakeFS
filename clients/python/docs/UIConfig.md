# UIConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**custom_viewers** | [**List[CustomViewer]**](CustomViewer.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.ui_config import UIConfig

# TODO update the JSON string below
json = "{}"
# create an instance of UIConfig from a JSON string
ui_config_instance = UIConfig.from_json(json)
# print the JSON string representation of the object
print UIConfig.to_json()

# convert the object into a dict
ui_config_dict = ui_config_instance.to_dict()
# create an instance of UIConfig from a dict
ui_config_form_dict = ui_config.from_dict(ui_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


