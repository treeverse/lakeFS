# lakefs_client.StagingApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_physical_address**](StagingApi.md#get_physical_address) | **GET** /repositories/{repository}/branches/{branch}/staging/backing | generate an address to which the client can upload an object
[**link_physical_address**](StagingApi.md#link_physical_address) | **PUT** /repositories/{repository}/branches/{branch}/staging/backing | associate staging on this physical address with a path


# **get_physical_address**
> StagingLocation get_physical_address(repository, branch, path)

generate an address to which the client can upload an object

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import staging_api
from lakefs_client.model.staging_location import StagingLocation
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
    api_instance = staging_api.StagingApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path = "path_example" # str | relative to the branch
    presign = True # bool |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # generate an address to which the client can upload an object
        api_response = api_instance.get_physical_address(repository, branch, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->get_physical_address: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # generate an address to which the client can upload an object
        api_response = api_instance.get_physical_address(repository, branch, path, presign=presign)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->get_physical_address: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path** | **str**| relative to the branch |
 **presign** | **bool**|  | [optional]

### Return type

[**StagingLocation**](StagingLocation.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | physical address for staging area |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **link_physical_address**
> ObjectStats link_physical_address(repository, branch, path, staging_metadata)

associate staging on this physical address with a path

Link the physical address with the path in lakeFS, creating an uncommitted change. The given address can be one generated by getPhysicalAddress, or an address outside the repository's storage namespace. 

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import staging_api
from lakefs_client.model.staging_location import StagingLocation
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
from lakefs_client.model.staging_metadata import StagingMetadata
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
    api_instance = staging_api.StagingApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path = "path_example" # str | relative to the branch
    staging_metadata = StagingMetadata(
        staging=StagingLocation(
            physical_address="physical_address_example",
            presigned_url="presigned_url_example",
            presigned_url_expiry=1,
        ),
        checksum="checksum_example",
        size_bytes=1,
        user_metadata={
            "key": "key_example",
        },
        content_type="content_type_example",
        force=False,
    ) # StagingMetadata | 
    if_none_match = "*" # str | Set to \"*\" to atomically allow the upload only if the key has no object yet. Other values are not supported. (optional)

    # example passing only required values which don't have defaults set
    try:
        # associate staging on this physical address with a path
        api_response = api_instance.link_physical_address(repository, branch, path, staging_metadata)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->link_physical_address: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # associate staging on this physical address with a path
        api_response = api_instance.link_physical_address(repository, branch, path, staging_metadata, if_none_match=if_none_match)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->link_physical_address: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path** | **str**| relative to the branch |
 **staging_metadata** | [**StagingMetadata**](StagingMetadata.md)|  |
 **if_none_match** | **str**| Set to \&quot;*\&quot; to atomically allow the upload only if the key has no object yet. Other values are not supported. | [optional]

### Return type

[**ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | object metadata |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Internal Server Error |  -  |
**409** | conflict with a commit, try here |  -  |
**412** | Precondition Failed |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

