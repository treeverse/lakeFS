# LoginConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**rbac** | Option<**String**> | RBAC will remain enabled on GUI if \"external\".  That only works with an external auth service.  | [optional]
**username_ui_placeholder** | Option<**String**> | Placeholder text to display in the username field of the login form.  | [optional]
**password_ui_placeholder** | Option<**String**> | Placeholder text to display in the password field of the login form.  | [optional]
**login_url** | **String** | Primary URL to use for login. | 
**login_flow** | Option<**String**> | Defines login behavior when login_url is set. - redirect (default): Auto-redirect to login_url. - selection: Show a page to choose between logging in via login_url or with lakeFS credentials. Ignored if login_url is not configured.  | [optional][default to Redirect]
**login_failed_message** | Option<**String**> | Message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  | [optional]
**fallback_login_url** | Option<**String**> | Secondary URL to offer users to use for login. | [optional]
**fallback_login_label** | Option<**String**> | Label to place on fallback_login_url. | [optional]
**login_cookie_names** | **Vec<String>** | Cookie names used to store JWT | 
**logout_url** | **String** | URL to use for logging out. | 
**use_login_placeholders** | Option<**bool**> | When set to true, the placeholders \"Username\" and \"Password\" are used in the login form. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


