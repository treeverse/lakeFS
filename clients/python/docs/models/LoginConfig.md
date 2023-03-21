# lakefs_client.model.login_config.LoginConfig

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**logout_url** | str,  | str,  | URL to use for logging out. | 
**login_url** | str,  | str,  | primary URL to use for login. | 
**[login_cookie_names](#login_cookie_names)** | list, tuple,  | tuple,  | cookie names used to store JWT | 
**RBAC** | str,  | str,  | RBAC will remain enabled on GUI if \&quot;external\&quot;.  That only works with an external auth service.  | [optional] must be one of ["simplified", "external", ] 
**login_failed_message** | str,  | str,  | message to display to users who fail to login; a full sentence that is rendered in HTML and may contain a link to a secondary login method  | [optional] 
**fallback_login_url** | str,  | str,  | secondary URL to offer users to use for login. | [optional] 
**fallback_login_label** | str,  | str,  | label to place on fallback_login_url. | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# login_cookie_names

cookie names used to store JWT

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  | cookie names used to store JWT | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
items | str,  | str,  |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

