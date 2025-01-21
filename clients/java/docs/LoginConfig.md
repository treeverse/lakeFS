

# LoginConfig


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**RBAC** | [**RBACEnum**](#RBACEnum) | RBAC will remain enabled on GUI if \&quot;external\&quot;.  That only works with an external auth service.  |  [optional] |
|**usernameUiPlaceholder** | **String** | Placeholder text to display in the username field of the login form.  |  [optional] |
|**passwordUiPlaceholder** | **String** | Placeholder text to display in the password field of the login form.  |  [optional] |
|**loginUrl** | **String** | primary URL to use for login. |  |
|**loginFailedMessage** | **String** | message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  |  [optional] |
|**fallbackLoginUrl** | **String** | secondary URL to offer users to use for login. |  [optional] |
|**fallbackLoginLabel** | **String** | label to place on fallback_login_url. |  [optional] |
|**loginCookieNames** | **List&lt;String&gt;** | cookie names used to store JWT |  |
|**logoutUrl** | **String** | URL to use for logging out. |  |



## Enum: RBACEnum

| Name | Value |
|---- | -----|
| NONE | &quot;none&quot; |
| SIMPLIFIED | &quot;simplified&quot; |
| EXTERNAL | &quot;external&quot; |



