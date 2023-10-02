# lakefs_sdk.OtfDiffApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_otf_diffs**](OtfDiffApi.md#get_otf_diffs) | **GET** /otf/diffs | get the available Open Table Format diffs
[**otf_diff**](OtfDiffApi.md#otf_diff) | **GET** /repositories/{repository}/otf/refs/{left_ref}/diff/{right_ref} | perform otf diff


# **get_otf_diffs**
> OTFDiffs get_otf_diffs()

get the available Open Table Format diffs

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
from lakefs_sdk.models.otf_diffs import OTFDiffs
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
    api_instance = lakefs_sdk.OtfDiffApi(api_client)

    try:
        # get the available Open Table Format diffs
        api_response = api_instance.get_otf_diffs()
        print("The response of OtfDiffApi->get_otf_diffs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OtfDiffApi->get_otf_diffs: %s\n" % e)
```



### Parameters
This endpoint does not need any parameter.

### Return type

[**OTFDiffs**](OTFDiffs.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | available Open Table Format diffs |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **otf_diff**
> OtfDiffList otf_diff(repository, left_ref, right_ref, table_path, type)

perform otf diff

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
from lakefs_sdk.models.otf_diff_list import OtfDiffList
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
    api_instance = lakefs_sdk.OtfDiffApi(api_client)
    repository = 'repository_example' # str | 
    left_ref = 'left_ref_example' # str | 
    right_ref = 'right_ref_example' # str | 
    table_path = 'table_path_example' # str | a path to the table location under the specified ref.
    type = 'type_example' # str | the type of otf

    try:
        # perform otf diff
        api_response = api_instance.otf_diff(repository, left_ref, right_ref, table_path, type)
        print("The response of OtfDiffApi->otf_diff:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OtfDiffApi->otf_diff: %s\n" % e)
```



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **left_ref** | **str**|  | 
 **right_ref** | **str**|  | 
 **table_path** | **str**| a path to the table location under the specified ref. | 
 **type** | **str**| the type of otf | 

### Return type

[**OtfDiffList**](OtfDiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

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

