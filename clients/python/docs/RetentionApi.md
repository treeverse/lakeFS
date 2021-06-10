# lakefs_client.RetentionApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**prepare_retention_commits**](RetentionApi.md#prepare_retention_commits) | **POST** /repositories/{repository}/gc/prepare | save lists of active and expired commits for garbage collection


# **prepare_retention_commits**
> InlineResponse201 prepare_retention_commits(repository)

save lists of active and expired commits for garbage collection

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import lakefs_client
from lakefs_client.api import retention_api
from lakefs_client.model.inline_response201 import InlineResponse201
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

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = retention_api.RetentionApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # save lists of active and expired commits for garbage collection
        api_response = api_instance.prepare_retention_commits(repository)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RetentionApi->prepare_retention_commits: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

[**InlineResponse201**](InlineResponse201.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | paths to commit sets |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

