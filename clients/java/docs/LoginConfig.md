

# LoginConfig


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**RBAC** | [**RBACEnum**](#RBACEnum) | RBAC will remain enabled on GUI if \&quot;external\&quot;.  That only works with an external auth service.  |  [optional] |
|**usernameUiPlaceholder** | **String** | Placeholder text to display in the username field of the login form.  |  [optional] |
|**passwordUiPlaceholder** | **String** | Placeholder text to display in the password field of the login form.  |  [optional] |
|**loginUrl** | **String** | Primary URL to use for login. |  |
|**loginUrlMethod** | [**LoginUrlMethodEnum**](#LoginUrlMethodEnum) | Defines login behavior when login_url is set. - none: For OSS users. - redirect: Auto-redirect to login_url. - select: Show a page to choose between logging in via login_url or with lakeFS credentials. Ignored if login_url is not configured.  |  [optional] |
|**loginFailedMessage** | **String** | Message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  |  [optional] |
|**fallbackLoginUrl** | **String** | Secondary URL to offer users to use for login. |  [optional] |
|**fallbackLoginLabel** | **String** | Label to place on fallback_login_url. |  [optional] |
|**loginCookieNames** | **List&lt;String&gt;** | Cookie names used to store JWT |  |
|**logoutUrl** | **String** | URL to use for logging out. |  |



## Enum: RBACEnum

| Name | Value |
|---- | -----|
| NONE | &quot;none&quot; |
| SIMPLIFIED | &quot;simplified&quot; |
| INTERNAL | &quot;internal&quot; |
| EXTERNAL | &quot;external&quot; |



## Enum: LoginUrlMethodEnum

| Name | Value |
|---- | -----|
| NONE | &quot;none&quot; |
| REDIRECT | &quot;redirect&quot; |
| SELECT | &quot;select&quot; |



