# lakefs_client.ObjectsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_object**](ObjectsApi.md#delete_object) | **DELETE** /repositories/{repository}/branches/{branch}/objects | delete object
[**delete_objects**](ObjectsApi.md#delete_objects) | **POST** /repositories/{repository}/branches/{branch}/objects/delete | delete objects
[**get_object**](ObjectsApi.md#get_object) | **GET** /repositories/{repository}/refs/{ref}/objects | get object content
[**get_underlying_properties**](ObjectsApi.md#get_underlying_properties) | **GET** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage
[**head_object**](ObjectsApi.md#head_object) | **HEAD** /repositories/{repository}/refs/{ref}/objects | check if object exists
[**list_objects**](ObjectsApi.md#list_objects) | **GET** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix
[**stage_object**](ObjectsApi.md#stage_object) | **PUT** /repositories/{repository}/branches/{branch}/objects | stage an object&#39;s metadata for the given branch
[**stat_object**](ObjectsApi.md#stat_object) | **GET** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata
[**upload_object**](ObjectsApi.md#upload_object) | **POST** /repositories/{repository}/branches/{branch}/objects | 


# **delete_object**
> delete_object(repository, branch, path)

delete object

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path = "path_example" # str | relative to the branch

    # example passing only required values which don't have defaults set
    try:
        # delete object
        api_instance.delete_object(repository, branch, path)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->delete_object: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path** | **str**| relative to the branch |

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
**204** | object deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_objects**
> ObjectErrorList delete_objects(repository, branch, path_list)

delete objects

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
from lakefs_client.model.object_error_list import ObjectErrorList
from lakefs_client.model.error import Error
from lakefs_client.model.path_list import PathList
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path_list = PathList(
        paths=[
            "paths_example",
        ],
    ) # PathList | 

    # example passing only required values which don't have defaults set
    try:
        # delete objects
        api_response = api_instance.delete_objects(repository, branch, path_list)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->delete_objects: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path_list** | [**PathList**](PathList.md)|  |

### Return type

[**ObjectErrorList**](ObjectErrorList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Delete objects response |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_object**
> file_type get_object(repository, ref, path)

get object content

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    ref = "ref_example" # str | a reference (could be either a branch or a commit ID)
    path = "path_example" # str | relative to the ref
    range = "bytes=0-1023" # str | Byte range to retrieve (optional)

    # example passing only required values which don't have defaults set
    try:
        # get object content
        api_response = api_instance.get_object(repository, ref, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->get_object: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # get object content
        api_response = api_instance.get_object(repository, ref, path, range=range)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->get_object: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**| a reference (could be either a branch or a commit ID) |
 **path** | **str**| relative to the ref |
 **range** | **str**| Byte range to retrieve | [optional]

### Return type

**file_type**

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream, application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | object content |  * Content-Length -  <br>  * Last-Modified -  <br>  * ETag -  <br>  |
**206** | partial object content |  * Content-Length -  <br>  * Content-Range -  <br>  * Last-Modified -  <br>  * ETag -  <br>  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**410** | object expired |  -  |
**416** | Requested Range Not Satisfiable |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_underlying_properties**
> UnderlyingObjectProperties get_underlying_properties(repository, ref, path)

get object properties on underlying storage

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
from lakefs_client.model.underlying_object_properties import UnderlyingObjectProperties
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    ref = "ref_example" # str | a reference (could be either a branch or a commit ID)
    path = "path_example" # str | relative to the branch

    # example passing only required values which don't have defaults set
    try:
        # get object properties on underlying storage
        api_response = api_instance.get_underlying_properties(repository, ref, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->get_underlying_properties: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**| a reference (could be either a branch or a commit ID) |
 **path** | **str**| relative to the branch |

### Return type

[**UnderlyingObjectProperties**](UnderlyingObjectProperties.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | object metadata on underlying storage |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **head_object**
> head_object(repository, ref, path)

check if object exists

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    ref = "ref_example" # str | a reference (could be either a branch or a commit ID)
    path = "path_example" # str | relative to the ref
    range = "bytes=0-1023" # str | Byte range to retrieve (optional)

    # example passing only required values which don't have defaults set
    try:
        # check if object exists
        api_instance.head_object(repository, ref, path)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->head_object: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # check if object exists
        api_instance.head_object(repository, ref, path, range=range)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->head_object: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**| a reference (could be either a branch or a commit ID) |
 **path** | **str**| relative to the ref |
 **range** | **str**| Byte range to retrieve | [optional]

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | object exists |  * Content-Length -  <br>  * Last-Modified -  <br>  * ETag -  <br>  |
**206** | partial object content info |  * Content-Length -  <br>  * Content-Range -  <br>  * Last-Modified -  <br>  * ETag -  <br>  |
**401** | Unauthorized |  -  |
**404** | object not found |  -  |
**410** | object expired |  -  |
**416** | Requested Range Not Satisfiable |  -  |
**0** | internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_objects**
> ObjectStatsList list_objects(repository, ref)

list objects under a given prefix

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
from lakefs_client.model.object_stats_list import ObjectStatsList
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    ref = "ref_example" # str | a reference (could be either a branch or a commit ID)
    user_metadata = True # bool |  (optional) if omitted the server will use the default value of True
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100
    delimiter = "delimiter_example" # str | delimiter used to group common prefixes by (optional)
    prefix = "prefix_example" # str | return items prefixed with this value (optional)

    # example passing only required values which don't have defaults set
    try:
        # list objects under a given prefix
        api_response = api_instance.list_objects(repository, ref)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->list_objects: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list objects under a given prefix
        api_response = api_instance.list_objects(repository, ref, user_metadata=user_metadata, after=after, amount=amount, delimiter=delimiter, prefix=prefix)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->list_objects: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**| a reference (could be either a branch or a commit ID) |
 **user_metadata** | **bool**|  | [optional] if omitted the server will use the default value of True
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100
 **delimiter** | **str**| delimiter used to group common prefixes by | [optional]
 **prefix** | **str**| return items prefixed with this value | [optional]

### Return type

[**ObjectStatsList**](ObjectStatsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | object listing |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stage_object**
> ObjectStats stage_object(repository, branch, path, object_stage_creation)

stage an object's metadata for the given branch

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
from lakefs_client.model.object_stage_creation import ObjectStageCreation
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path = "path_example" # str | relative to the branch
    object_stage_creation = ObjectStageCreation(
        physical_address="physical_address_example",
        checksum="checksum_example",
        size_bytes=1,
        mtime=1,
        metadata=ObjectUserMetadata(
            key="key_example",
        ),
        content_type="content_type_example",
    ) # ObjectStageCreation | 

    # example passing only required values which don't have defaults set
    try:
        # stage an object's metadata for the given branch
        api_response = api_instance.stage_object(repository, branch, path, object_stage_creation)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->stage_object: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path** | **str**| relative to the branch |
 **object_stage_creation** | [**ObjectStageCreation**](ObjectStageCreation.md)|  |

### Return type

[**ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | object metadata |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stat_object**
> ObjectStats stat_object(repository, ref, path)

get object metadata

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    ref = "ref_example" # str | a reference (could be either a branch or a commit ID)
    path = "path_example" # str | relative to the branch
    user_metadata = True # bool |  (optional) if omitted the server will use the default value of True

    # example passing only required values which don't have defaults set
    try:
        # get object metadata
        api_response = api_instance.stat_object(repository, ref, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->stat_object: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # get object metadata
        api_response = api_instance.stat_object(repository, ref, path, user_metadata=user_metadata)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->stat_object: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**| a reference (could be either a branch or a commit ID) |
 **path** | **str**| relative to the branch |
 **user_metadata** | **bool**|  | [optional] if omitted the server will use the default value of True

### Return type

[**ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | object metadata |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**410** | object gone (but partial metadata may be available) |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_object**
> ObjectStats upload_object(repository, branch, path)



### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import objects_api
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path = "path_example" # str | relative to the branch
    storage_class = "storageClass_example" # str |  (optional)
    if_none_match = "*" # str | Currently supports only \"*\" to allow uploading an object only if one doesn't exist yet (optional)
    content = open('/path/to/file', 'rb') # file_type | Only a single file per upload which must be named \\\"content\\\". (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.upload_object(repository, branch, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->upload_object: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.upload_object(repository, branch, path, storage_class=storage_class, if_none_match=if_none_match, content=content)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->upload_object: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path** | **str**| relative to the branch |
 **storage_class** | **str**|  | [optional]
 **if_none_match** | **str**| Currently supports only \&quot;*\&quot; to allow uploading an object only if one doesn&#39;t exist yet | [optional]
 **content** | **file_type**| Only a single file per upload which must be named \\\&quot;content\\\&quot;. | [optional]

### Return type

[**ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | object metadata |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**412** | Precondition Failed |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

