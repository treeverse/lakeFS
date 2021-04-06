# lakefs.RepositoriesApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_repository**](RepositoriesApi.md#create_repository) | **POST** /repositories | create repository
[**delete_repository**](RepositoriesApi.md#delete_repository) | **DELETE** /repositories/{repository} | delete repository
[**get_repository**](RepositoriesApi.md#get_repository) | **GET** /repositories/{repository} | get repository
[**list_repositories**](RepositoriesApi.md#list_repositories) | **GET** /repositories | list repositories


# **create_repository**
> Repository create_repository(repository_creation)

create repository

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import repositories_api
from lakefs.model.error import Error
from lakefs.model.repository import Repository
from lakefs.model.repository_creation import RepositoryCreation
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: jwt_token
configuration.api_key['jwt_token'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['jwt_token'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = repositories_api.RepositoriesApi(api_client)
    repository_creation = RepositoryCreation(
        name="wr1c2v7s6djuy1zmeto",
        storage_namespace="s3://example-bucket/",
        default_branch="main",
    ) # RepositoryCreation | 
    bare = False # bool | If true, create a bare repository with no initial commit and branch (optional) if omitted the server will use the default value of False

    # example passing only required values which don't have defaults set
    try:
        # create repository
        api_response = api_instance.create_repository(repository_creation)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling RepositoriesApi->create_repository: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # create repository
        api_response = api_instance.create_repository(repository_creation, bare=bare)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling RepositoriesApi->create_repository: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository_creation** | [**RepositoryCreation**](RepositoryCreation.md)|  |
 **bare** | **bool**| If true, create a bare repository with no initial commit and branch | [optional] if omitted the server will use the default value of False

### Return type

[**Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | repository |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_repository**
> delete_repository(repository)

delete repository

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import repositories_api
from lakefs.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: jwt_token
configuration.api_key['jwt_token'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['jwt_token'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = repositories_api.RepositoriesApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete repository
        api_instance.delete_repository(repository)
    except lakefs.ApiException as e:
        print("Exception when calling RepositoriesApi->delete_repository: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | repository deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_repository**
> Repository get_repository(repository)

get repository

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import repositories_api
from lakefs.model.error import Error
from lakefs.model.repository import Repository
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: jwt_token
configuration.api_key['jwt_token'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['jwt_token'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = repositories_api.RepositoriesApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get repository
        api_response = api_instance.get_repository(repository)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling RepositoriesApi->get_repository: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

[**Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | repository |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_repositories**
> RepositoryList list_repositories()

list repositories

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import repositories_api
from lakefs.model.error import Error
from lakefs.model.repository_list import RepositoryList
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: jwt_token
configuration.api_key['jwt_token'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['jwt_token'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = repositories_api.RepositoriesApi(api_client)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list repositories
        api_response = api_instance.list_repositories(after=after, amount=amount)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling RepositoriesApi->list_repositories: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**RepositoryList**](RepositoryList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | repository list |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

