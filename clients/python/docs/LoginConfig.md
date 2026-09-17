# LoginConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**login_failed_message** | **str** | Message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  | [optional] 

## Example

```python
from lakefs_sdk.models.login_config import LoginConfig

# TODO update the JSON string below
json = "{}"
# create an instance of LoginConfig from a JSON string
login_config_instance = LoginConfig.from_json(json)
# print the JSON string representation of the object
print LoginConfig.to_json()

# convert the object into a dict
login_config_dict = login_config_instance.to_dict()
# create an instance of LoginConfig from a dict
login_config_form_dict = login_config.from_dict(login_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


