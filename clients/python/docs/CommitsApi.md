# lakefs.CommitsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**commit**](CommitsApi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit
[**get_commit**](CommitsApi.md#get_commit) | **GET** /repositories/{repository}/commits/{commitId} | get commit
[**log_branch_commits**](CommitsApi.md#log_branch_commits) | **GET** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 


# **commit**
> Commit commit(repository, branch, commit_creation)

create commit

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import commits_api
from lakefs.model.commit import Commit
from lakefs.model.commit_creation import CommitCreation
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
    api_instance = commits_api.CommitsApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    commit_creation = CommitCreation(
        message="message_example",
        metadata={
            "key": "key_example",
        },
    ) # CommitCreation | 

    # example passing only required values which don't have defaults set
    try:
        # create commit
        api_response = api_instance.commit(repository, branch, commit_creation)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling CommitsApi->commit: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **commit_creation** | [**CommitCreation**](CommitCreation.md)|  |

### Return type

[**Commit**](Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | commit |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**412** | Precondition Failed (e.g. a pre-commit hook returned a failure) |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_commit**
> Commit get_commit(repository, commit_id)

get commit

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import commits_api
from lakefs.model.commit import Commit
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
    api_instance = commits_api.CommitsApi(api_client)
    repository = "repository_example" # str | 
    commit_id = "commitId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get commit
        api_response = api_instance.get_commit(repository, commit_id)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling CommitsApi->get_commit: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **commit_id** | **str**|  |

### Return type

[**Commit**](Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | commit |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **log_branch_commits**
> CommitList log_branch_commits(repository, branch)

get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (jwt_token):
```python
import time
import lakefs
from lakefs.api import commits_api
from lakefs.model.error import Error
from lakefs.model.commit_list import CommitList
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
    api_instance = commits_api.CommitsApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    try:
        # get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
        api_response = api_instance.log_branch_commits(repository, branch)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling CommitsApi->log_branch_commits: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
        api_response = api_instance.log_branch_commits(repository, branch, after=after, amount=amount)
        pprint(api_response)
    except lakefs.ApiException as e:
        print("Exception when calling CommitsApi->log_branch_commits: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | commit log |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

