# LoginConfig


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**login_url** | **str** | primary URL to use for login. | 
**login_cookies** | **[str]** | cookies to store JWT | 
**logout_url** | **str** | URL to use for logging out. | 
**rbac** | **str** | RBAC will remain enabled on GUI if \&quot;external\&quot;.  That only works with an external auth service.  | [optional] 
**login_failed_message** | **str** | message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  | [optional] 
**fallback_login_url** | **str** | secondary URL to offer users to use for login. | [optional] 
**fallback_login_label** | **str** | label to place on fallback_login_url. | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


