# lakefs_client.RetentionApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_garbage_collection_rules**](RetentionApi.md#delete_garbage_collection_rules) | **DELETE** /repositories/{repository}/gc/rules | 
[**get_garbage_collection_rules**](RetentionApi.md#get_garbage_collection_rules) | **GET** /repositories/{repository}/gc/rules | 
[**prepare_garbage_collection_commits**](RetentionApi.md#prepare_garbage_collection_commits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active and expired commits for garbage collection
[**prepare_garbage_collection_uncommitted**](RetentionApi.md#prepare_garbage_collection_uncommitted) | **POST** /repositories/{repository}/gc/prepare_uncommited | save repository uncommitted metadata for garbage collection
[**set_garbage_collection_rules**](RetentionApi.md#set_garbage_collection_rules) | **POST** /repositories/{repository}/gc/rules | 
[**set_garbage_collection_rules_preflight**](RetentionApi.md#set_garbage_collection_rules_preflight) | **GET** /repositories/{repository}/gc/rules/set_allowed | 


# **delete_garbage_collection_rules**
> delete_garbage_collection_rules(repository)



### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import retention_api
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_instance.delete_garbage_collection_rules(repository)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->delete_garbage_collection_rules: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | deleted garbage collection rules successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_garbage_collection_rules**
> GarbageCollectionRules get_garbage_collection_rules(repository)



### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import retention_api
from lakefs_client.model.garbage_collection_rules import GarbageCollectionRules
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_garbage_collection_rules(repository)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->get_garbage_collection_rules: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

[**GarbageCollectionRules**](GarbageCollectionRules.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | gc rule list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **prepare_garbage_collection_commits**
> GarbageCollectionPrepareResponse prepare_garbage_collection_commits(repository)

save lists of active and expired commits for garbage collection

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import retention_api
from lakefs_client.model.error import Error
from lakefs_client.model.garbage_collection_prepare_response import GarbageCollectionPrepareResponse
from lakefs_client.model.garbage_collection_prepare_request import GarbageCollectionPrepareRequest
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 
    garbage_collection_prepare_request = GarbageCollectionPrepareRequest(
        previous_run_id="64eaa103-d726-4a33-bcb8-7c0b4abfe09e",
    ) # GarbageCollectionPrepareRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # save lists of active and expired commits for garbage collection
        api_response = api_instance.prepare_garbage_collection_commits(repository)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->prepare_garbage_collection_commits: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # save lists of active and expired commits for garbage collection
        api_response = api_instance.prepare_garbage_collection_commits(repository, garbage_collection_prepare_request=garbage_collection_prepare_request)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->prepare_garbage_collection_commits: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **garbage_collection_prepare_request** | [**GarbageCollectionPrepareRequest**](GarbageCollectionPrepareRequest.md)|  | [optional]

### Return type

[**GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | paths to commit dataset |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **prepare_garbage_collection_uncommitted**
> PrepareGCUncommittedResponse prepare_garbage_collection_uncommitted(repository)

save repository uncommitted metadata for garbage collection

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import retention_api
from lakefs_client.model.prepare_gc_uncommitted_response import PrepareGCUncommittedResponse
from lakefs_client.model.prepare_gc_uncommitted_request import PrepareGCUncommittedRequest
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 
    prepare_gc_uncommitted_request = PrepareGCUncommittedRequest(
        continuation_token="continuation_token_example",
    ) # PrepareGCUncommittedRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # save repository uncommitted metadata for garbage collection
        api_response = api_instance.prepare_garbage_collection_uncommitted(repository)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->prepare_garbage_collection_uncommitted: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # save repository uncommitted metadata for garbage collection
        api_response = api_instance.prepare_garbage_collection_uncommitted(repository, prepare_gc_uncommitted_request=prepare_gc_uncommitted_request)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->prepare_garbage_collection_uncommitted: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **prepare_gc_uncommitted_request** | [**PrepareGCUncommittedRequest**](PrepareGCUncommittedRequest.md)|  | [optional]

### Return type

[**PrepareGCUncommittedResponse**](PrepareGCUncommittedResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | paths to commit dataset |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_garbage_collection_rules**
> set_garbage_collection_rules(repository, garbage_collection_rules)



### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import retention_api
from lakefs_client.model.garbage_collection_rules import GarbageCollectionRules
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 
    garbage_collection_rules = GarbageCollectionRules(
        default_retention_days=1,
        branches=[
            GarbageCollectionRule(
                branch_id="branch_id_example",
                retention_days=1,
            ),
        ],
    ) # GarbageCollectionRules | 

    # example passing only required values which don't have defaults set
    try:
        api_instance.set_garbage_collection_rules(repository, garbage_collection_rules)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->set_garbage_collection_rules: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **garbage_collection_rules** | [**GarbageCollectionRules**](GarbageCollectionRules.md)|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | set garbage collection rules successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_garbage_collection_rules_preflight**
> set_garbage_collection_rules_preflight(repository)



### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import retention_api
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_instance.set_garbage_collection_rules_preflight(repository)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->set_garbage_collection_rules_preflight: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | User has permissions to set garbage collection rules on this repository |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

