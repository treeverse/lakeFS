# lakefs_client.ExperimentalApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**otf_diff**](ExperimentalApi.md#otf_diff) | **GET** /repositories/{repository}/otf/refs/{left_ref}/diff/{right_ref} | perform otf diff


# **otf_diff**
> OtfDiffList otf_diff(repository, left_ref, right_ref, table_path, type)

perform otf diff

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.otf_diff_list import OtfDiffList
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
    api_instance = experimental_api.ExperimentalApi(api_client)
    repository = "repository_example" # str | 
    left_ref = "left_ref_example" # str | 
    right_ref = "right_ref_example" # str | 
    table_path = "table_path_example" # str | a path to the table location under the specified ref.
    type = "type_example" # str | the type of otf
    base_ref = "base_ref_example" # str | base ref to compare a three way diff (optional)

    # example passing only required values which don't have defaults set
    try:
        # perform otf diff
        api_response = api_instance.otf_diff(repository, left_ref, right_ref, table_path, type)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->otf_diff: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # perform otf diff
        api_response = api_instance.otf_diff(repository, left_ref, right_ref, table_path, type, base_ref=base_ref)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->otf_diff: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **left_ref** | **str**|  |
 **right_ref** | **str**|  |
 **table_path** | **str**| a path to the table location under the specified ref. |
 **type** | **str**| the type of otf |
 **base_ref** | **str**| base ref to compare a three way diff | [optional]

### Return type

[**OtfDiffList**](OtfDiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | diff list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**412** | Precondition Failed |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

