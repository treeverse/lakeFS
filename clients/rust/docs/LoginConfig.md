# LoginConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**rbac** | Option<**String**> | RBAC will remain enabled on GUI if \"external\".  That only works with an external auth service.  | [optional]
**login_url** | **String** | primary URL to use for login. | 
**login_failed_message** | Option<**String**> | message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  | [optional]
**fallback_login_url** | Option<**String**> | secondary URL to offer users to use for login. | [optional]
**fallback_login_label** | Option<**String**> | label to place on fallback_login_url. | [optional]
**login_cookie_names** | **Vec<String>** | cookie names used to store JWT | 
**logout_url** | **String** | URL to use for logging out. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


