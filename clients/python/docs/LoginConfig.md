# LoginConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**rbac** | **str** | RBAC will remain enabled on GUI if \&quot;external\&quot;.  That only works with an external auth service.  | [optional] 
**username_ui_placeholder** | **str** | Placeholder text to display in the username field of the login form.  | [optional] 
**password_ui_placeholder** | **str** | Placeholder text to display in the password field of the login form.  | [optional] 
**login_url** | **str** | Primary URL to use for login. | 
**login_flow** | **str** | Defines login behavior when login_url is set. - redirect (default): Auto-redirect to login_url. - selection: Show a page to choose between logging in via login_url or with lakeFS credentials. Ignored if login_url is not configured.  | [optional] [default to 'redirect']
**login_failed_message** | **str** | Message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  | [optional] 
**fallback_login_url** | **str** | Secondary URL to offer users to use for login. | [optional] 
**fallback_login_label** | **str** | Label to place on fallback_login_url. | [optional] 
**login_cookie_names** | **List[str]** | Cookie names used to store JWT | 
**logout_url** | **str** | URL to use for logging out. | 
**use_login_placeholders** | **bool** | When set to true, the placeholders \&quot;Username\&quot; and \&quot;Password\&quot; are used in the login form. | [optional] 

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


