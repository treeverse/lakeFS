# lakefs_sdk.RetentionApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**prepare_garbage_collection_commits**](RetentionApi.md#prepare_garbage_collection_commits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active commits for garbage collection
[**prepare_garbage_collection_uncommitted**](RetentionApi.md#prepare_garbage_collection_uncommitted) | **POST** /repositories/{repository}/gc/prepare_uncommited | save repository uncommitted metadata for garbage collection


# **prepare_garbage_collection_commits**
> GarbageCollectionPrepareResponse prepare_garbage_collection_commits(repository)

save lists of active commits for garbage collection

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.garbage_collection_prepare_response import GarbageCollectionPrepareResponse
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.RetentionApi(api_client)
    repository = 'repository_example' # str | 

    try:
        # save lists of active commits for garbage collection
        api_response = api_instance.prepare_garbage_collection_commits(repository)
        print("The response of RetentionApi->prepare_garbage_collection_commits:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling RetentionApi->prepare_garbage_collection_commits: %s\n" % e)
```



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 

### Return type

[**GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
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
> PrepareGCUncommittedResponse prepare_garbage_collection_uncommitted(repository, prepare_gc_uncommitted_request=prepare_gc_uncommitted_request)

save repository uncommitted metadata for garbage collection

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.prepare_gc_uncommitted_request import PrepareGCUncommittedRequest
from lakefs_sdk.models.prepare_gc_uncommitted_response import PrepareGCUncommittedResponse
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.RetentionApi(api_client)
    repository = 'repository_example' # str | 
    prepare_gc_uncommitted_request = lakefs_sdk.PrepareGCUncommittedRequest() # PrepareGCUncommittedRequest |  (optional)

    try:
        # save repository uncommitted metadata for garbage collection
        api_response = api_instance.prepare_garbage_collection_uncommitted(repository, prepare_gc_uncommitted_request=prepare_gc_uncommitted_request)
        print("The response of RetentionApi->prepare_garbage_collection_uncommitted:\n")
        pprint(api_response)
    except Exception as e:
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

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

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

