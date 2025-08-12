

# LoginConfig


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**RBAC** | [**RBACEnum**](#RBACEnum) | RBAC will remain enabled on GUI if \&quot;external\&quot;.  That only works with an external auth service.  |  [optional] |
|**selectLoginMethod** | **Boolean** | When set to true, displays a login page that lets the user choose a preferred authentication method for  logging into lakeFS. Either via SSO (using the login_url field, which needs to be configured) or using  lakeFS credentials.  |  [optional] |
|**usernameUiPlaceholder** | **String** | Placeholder text to display in the username field of the login form.  |  [optional] |
|**passwordUiPlaceholder** | **String** | Placeholder text to display in the password field of the login form.  |  [optional] |
|**loginUrl** | **String** | Primary URL to use for login. |  |
|**loginFailedMessage** | **String** | Message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  |  [optional] |
|**fallbackLoginUrl** | **String** | Secondary URL to offer users to use for login. |  [optional] |
|**fallbackLoginLabel** | **String** | Label to place on fallback_login_url. |  [optional] |
|**loginCookieNames** | **List&lt;String&gt;** | Cookie names used to store JWT |  |
|**logoutUrl** | **String** | URL to use for logging out. |  |
|**useLoginPlaceholders** | **Boolean** | When set to true, the placeholders \&quot;Username\&quot; and \&quot;Password\&quot; are used in the login form. |  [optional] |



## Enum: RBACEnum

| Name | Value |
|---- | -----|
| NONE | &quot;none&quot; |
| SIMPLIFIED | &quot;simplified&quot; |
| INTERNAL | &quot;internal&quot; |
| EXTERNAL | &quot;external&quot; |



