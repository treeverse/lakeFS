# lakefs_client.AuthApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_group_membership**](AuthApi.md#add_group_membership) | **PUT** /auth/groups/{groupId}/members/{userId} | add group membership
[**attach_policy_to_group**](AuthApi.md#attach_policy_to_group) | **PUT** /auth/groups/{groupId}/policies/{policyId} | attach policy to group
[**attach_policy_to_user**](AuthApi.md#attach_policy_to_user) | **PUT** /auth/users/{userId}/policies/{policyId} | attach policy to user
[**create_credentials**](AuthApi.md#create_credentials) | **POST** /auth/users/{userId}/credentials | create credentials
[**create_group**](AuthApi.md#create_group) | **POST** /auth/groups | create group
[**create_policy**](AuthApi.md#create_policy) | **POST** /auth/policies | create policy
[**create_user**](AuthApi.md#create_user) | **POST** /auth/users | create user
[**delete_credentials**](AuthApi.md#delete_credentials) | **DELETE** /auth/users/{userId}/credentials/{accessKeyId} | delete credentials
[**delete_group**](AuthApi.md#delete_group) | **DELETE** /auth/groups/{groupId} | delete group
[**delete_group_membership**](AuthApi.md#delete_group_membership) | **DELETE** /auth/groups/{groupId}/members/{userId} | delete group membership
[**delete_policy**](AuthApi.md#delete_policy) | **DELETE** /auth/policies/{policyId} | delete policy
[**delete_user**](AuthApi.md#delete_user) | **DELETE** /auth/users/{userId} | delete user
[**detach_policy_from_group**](AuthApi.md#detach_policy_from_group) | **DELETE** /auth/groups/{groupId}/policies/{policyId} | detach policy from group
[**detach_policy_from_user**](AuthApi.md#detach_policy_from_user) | **DELETE** /auth/users/{userId}/policies/{policyId} | detach policy from user
[**forgot_password**](AuthApi.md#forgot_password) | **POST** /auth/password/forgot | forgot password request initiates the password reset process
[**get_auth_capabilities**](AuthApi.md#get_auth_capabilities) | **GET** /auth/capabilities | list authentication capabilities supported
[**get_credentials**](AuthApi.md#get_credentials) | **GET** /auth/users/{userId}/credentials/{accessKeyId} | get credentials
[**get_current_user**](AuthApi.md#get_current_user) | **GET** /user | get current user
[**get_group**](AuthApi.md#get_group) | **GET** /auth/groups/{groupId} | get group
[**get_policy**](AuthApi.md#get_policy) | **GET** /auth/policies/{policyId} | get policy
[**get_user**](AuthApi.md#get_user) | **GET** /auth/users/{userId} | get user
[**list_group_members**](AuthApi.md#list_group_members) | **GET** /auth/groups/{groupId}/members | list group members
[**list_group_policies**](AuthApi.md#list_group_policies) | **GET** /auth/groups/{groupId}/policies | list group policies
[**list_groups**](AuthApi.md#list_groups) | **GET** /auth/groups | list groups
[**list_policies**](AuthApi.md#list_policies) | **GET** /auth/policies | list policies
[**list_user_credentials**](AuthApi.md#list_user_credentials) | **GET** /auth/users/{userId}/credentials | list user credentials
[**list_user_groups**](AuthApi.md#list_user_groups) | **GET** /auth/users/{userId}/groups | list user groups
[**list_user_policies**](AuthApi.md#list_user_policies) | **GET** /auth/users/{userId}/policies | list user policies
[**list_users**](AuthApi.md#list_users) | **GET** /auth/users | list users
[**login**](AuthApi.md#login) | **POST** /auth/login | perform a login
[**update_password**](AuthApi.md#update_password) | **POST** /auth/password | Update user password by reset_password token
[**update_policy**](AuthApi.md#update_policy) | **PUT** /auth/policies/{policyId} | update policy


# **add_group_membership**
> add_group_membership(group_id, user_id)

add group membership

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 
    user_id = "userId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # add group membership
        api_instance.add_group_membership(group_id, user_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->add_group_membership: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |
 **user_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | membership added successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **attach_policy_to_group**
> attach_policy_to_group(group_id, policy_id)

attach policy to group

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 
    policy_id = "policyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # attach policy to group
        api_instance.attach_policy_to_group(group_id, policy_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->attach_policy_to_group: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |
 **policy_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | policy attached successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **attach_policy_to_user**
> attach_policy_to_user(user_id, policy_id)

attach policy to user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    policy_id = "policyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # attach policy to user
        api_instance.attach_policy_to_user(user_id, policy_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->attach_policy_to_user: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **policy_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | policy attached successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_credentials**
> CredentialsWithSecret create_credentials(user_id)

create credentials

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.credentials_with_secret import CredentialsWithSecret
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # create credentials
        api_response = api_instance.create_credentials(user_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->create_credentials: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |

### Return type

[**CredentialsWithSecret**](CredentialsWithSecret.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | credentials |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_group**
> Group create_group()

create group

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.group_creation import GroupCreation
from lakefs_client.model.error import Error
from lakefs_client.model.group import Group
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_creation = GroupCreation(
        id="id_example",
    ) # GroupCreation |  (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # create group
        api_response = api_instance.create_group(group_creation=group_creation)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->create_group: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_creation** | [**GroupCreation**](GroupCreation.md)|  | [optional]

### Return type

[**Group**](Group.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | group |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_policy**
> Policy create_policy(policy)

create policy

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.policy import Policy
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    policy = Policy(
        id="id_example",
        creation_date=1,
        statement=[
            Statement(
                effect="allow",
                resource="resource_example",
                action=[
                    "action_example",
                ],
            ),
        ],
    ) # Policy | 

    # example passing only required values which don't have defaults set
    try:
        # create policy
        api_response = api_instance.create_policy(policy)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->create_policy: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policy** | [**Policy**](Policy.md)|  |

### Return type

[**Policy**](Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | policy |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**409** | Resource Conflicts With Target |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_user**
> User create_user()

create user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from lakefs_client.model.user import User
from lakefs_client.model.user_creation import UserCreation
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_creation = UserCreation(
        id="id_example",
        invite_user=True,
    ) # UserCreation |  (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # create user
        api_response = api_instance.create_user(user_creation=user_creation)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->create_user: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_creation** | [**UserCreation**](UserCreation.md)|  | [optional]

### Return type

[**User**](User.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | user |  -  |
**400** | validation error |  -  |
**401** | Unauthorized |  -  |
**409** | Resource Conflicts With Target |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_credentials**
> delete_credentials(user_id, access_key_id)

delete credentials

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    access_key_id = "accessKeyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete credentials
        api_instance.delete_credentials(user_id, access_key_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->delete_credentials: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **access_key_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | credentials deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_group**
> delete_group(group_id)

delete group

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete group
        api_instance.delete_group(group_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->delete_group: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | group deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_group_membership**
> delete_group_membership(group_id, user_id)

delete group membership

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 
    user_id = "userId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete group membership
        api_instance.delete_group_membership(group_id, user_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->delete_group_membership: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |
 **user_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | membership deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_policy**
> delete_policy(policy_id)

delete policy

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    policy_id = "policyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete policy
        api_instance.delete_policy(policy_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->delete_policy: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policy_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | policy deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_user**
> delete_user(user_id)

delete user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete user
        api_instance.delete_user(user_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->delete_user: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | user deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **detach_policy_from_group**
> detach_policy_from_group(group_id, policy_id)

detach policy from group

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 
    policy_id = "policyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # detach policy from group
        api_instance.detach_policy_from_group(group_id, policy_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->detach_policy_from_group: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |
 **policy_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | policy detached successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **detach_policy_from_user**
> detach_policy_from_user(user_id, policy_id)

detach policy from user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    policy_id = "policyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # detach policy from user
        api_instance.detach_policy_from_user(user_id, policy_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->detach_policy_from_user: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **policy_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | policy detached successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **forgot_password**
> forgot_password(forgot_password_request)

forgot password request initiates the password reset process

### Example


```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.forgot_password_request import ForgotPasswordRequest
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    forgot_password_request = ForgotPasswordRequest(
        email="email_example",
    ) # ForgotPasswordRequest | 

    # example passing only required values which don't have defaults set
    try:
        # forgot password request initiates the password reset process
        api_instance.forgot_password(forgot_password_request)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->forgot_password: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **forgot_password_request** | [**ForgotPasswordRequest**](ForgotPasswordRequest.md)|  |

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | No content |  -  |
**400** | bad request |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_auth_capabilities**
> AuthCapabilities get_auth_capabilities()

list authentication capabilities supported

### Example


```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from lakefs_client.model.auth_capabilities import AuthCapabilities
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # list authentication capabilities supported
        api_response = api_instance.get_auth_capabilities()
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->get_auth_capabilities: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

[**AuthCapabilities**](AuthCapabilities.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | auth capabilities |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_credentials**
> Credentials get_credentials(user_id, access_key_id)

get credentials

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.credentials import Credentials
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    access_key_id = "accessKeyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get credentials
        api_response = api_instance.get_credentials(user_id, access_key_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->get_credentials: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **access_key_id** | **str**|  |

### Return type

[**Credentials**](Credentials.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | credentials |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_current_user**
> CurrentUser get_current_user()

get current user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.current_user import CurrentUser
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # get current user
        api_response = api_instance.get_current_user()
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->get_current_user: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

[**CurrentUser**](CurrentUser.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | user |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_group**
> Group get_group(group_id)

get group

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from lakefs_client.model.group import Group
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get group
        api_response = api_instance.get_group(group_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->get_group: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |

### Return type

[**Group**](Group.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | group |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_policy**
> Policy get_policy(policy_id)

get policy

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.policy import Policy
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    policy_id = "policyId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get policy
        api_response = api_instance.get_policy(policy_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->get_policy: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policy_id** | **str**|  |

### Return type

[**Policy**](Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | policy |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_user**
> User get_user(user_id)

get user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from lakefs_client.model.user import User
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get user
        api_response = api_instance.get_user(user_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->get_user: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |

### Return type

[**User**](User.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | user |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_group_members**
> UserList list_group_members(group_id)

list group members

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.user_list import UserList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    try:
        # list group members
        api_response = api_instance.list_group_members(group_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_group_members: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list group members
        api_response = api_instance.list_group_members(group_id, prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_group_members: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**UserList**](UserList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | group memeber list |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_group_policies**
> PolicyList list_group_policies(group_id)

list group policies

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.policy_list import PolicyList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    group_id = "groupId_example" # str | 
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    try:
        # list group policies
        api_response = api_instance.list_group_policies(group_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_group_policies: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list group policies
        api_response = api_instance.list_group_policies(group_id, prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_group_policies: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **group_id** | **str**|  |
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**PolicyList**](PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | policy list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_groups**
> GroupList list_groups()

list groups

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.group_list import GroupList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list groups
        api_response = api_instance.list_groups(prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_groups: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**GroupList**](GroupList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | group list |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_policies**
> PolicyList list_policies()

list policies

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.policy_list import PolicyList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list policies
        api_response = api_instance.list_policies(prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_policies: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**PolicyList**](PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | policy list |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_user_credentials**
> CredentialsList list_user_credentials(user_id)

list user credentials

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.error import Error
from lakefs_client.model.credentials_list import CredentialsList
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    try:
        # list user credentials
        api_response = api_instance.list_user_credentials(user_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_user_credentials: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list user credentials
        api_response = api_instance.list_user_credentials(user_id, prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_user_credentials: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**CredentialsList**](CredentialsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | credential list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_user_groups**
> GroupList list_user_groups(user_id)

list user groups

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.group_list import GroupList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    try:
        # list user groups
        api_response = api_instance.list_user_groups(user_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_user_groups: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list user groups
        api_response = api_instance.list_user_groups(user_id, prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_user_groups: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**GroupList**](GroupList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | group list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_user_policies**
> PolicyList list_user_policies(user_id)

list user policies

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.policy_list import PolicyList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    user_id = "userId_example" # str | 
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100
    effective = False # bool | will return all distinct policies attached to the user or any of its groups (optional) if omitted the server will use the default value of False

    # example passing only required values which don't have defaults set
    try:
        # list user policies
        api_response = api_instance.list_user_policies(user_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_user_policies: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list user policies
        api_response = api_instance.list_user_policies(user_id, prefix=prefix, after=after, amount=amount, effective=effective)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_user_policies: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100
 **effective** | **bool**| will return all distinct policies attached to the user or any of its groups | [optional] if omitted the server will use the default value of False

### Return type

[**PolicyList**](PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | policy list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_users**
> UserList list_users()

list users

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.user_list import UserList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list users
        api_response = api_instance.list_users(prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->list_users: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**UserList**](UserList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | user list |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **login**
> AuthenticationToken login()

perform a login

### Example


```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.authentication_token import AuthenticationToken
from lakefs_client.model.login_information import LoginInformation
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    login_information = LoginInformation(
        access_key_id="access_key_id_example",
        secret_access_key="secret_access_key_example",
    ) # LoginInformation |  (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # perform a login
        api_response = api_instance.login(login_information=login_information)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->login: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **login_information** | [**LoginInformation**](LoginInformation.md)|  | [optional]

### Return type

[**AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful login |  * Set-Cookie -  <br>  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_password**
> update_password(update_password_by_token)

Update user password by reset_password token

### Example

* Api Key Authentication (cookie_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.update_password_by_token import UpdatePasswordByToken
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    update_password_by_token = UpdatePasswordByToken(
        token="token_example",
        new_password="new_password_example",
        email="email_example",
    ) # UpdatePasswordByToken | 

    # example passing only required values which don't have defaults set
    try:
        # Update user password by reset_password token
        api_instance.update_password(update_password_by_token)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->update_password: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **update_password_by_token** | [**UpdatePasswordByToken**](UpdatePasswordByToken.md)|  |

### Return type

void (empty response body)

### Authorization

[cookie_auth](../README.md#cookie_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | successful reset |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_policy**
> Policy update_policy(policy_id, policy)

update policy

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import auth_api
from lakefs_client.model.policy import Policy
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = auth_api.AuthApi(api_client)
    policy_id = "policyId_example" # str | 
    policy = Policy(
        id="id_example",
        creation_date=1,
        statement=[
            Statement(
                effect="allow",
                resource="resource_example",
                action=[
                    "action_example",
                ],
            ),
        ],
    ) # Policy | 

    # example passing only required values which don't have defaults set
    try:
        # update policy
        api_response = api_instance.update_policy(policy_id, policy)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling AuthApi->update_policy: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policy_id** | **str**|  |
 **policy** | [**Policy**](Policy.md)|  |

### Return type

[**Policy**](Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | policy |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

