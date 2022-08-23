# lakefs_client.RefsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**diff_refs**](RefsApi.md#diff_refs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
[**dump_refs**](RefsApi.md#dump_refs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs (tags, commits, branches) to object store
[**log_commits**](RefsApi.md#log_commits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref. If both objects and prefixes are empty, return all commits.
[**merge_into_branch**](RefsApi.md#merge_into_branch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references
[**restore_refs**](RefsApi.md#restore_refs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs (tags, commits, branches) from object store


# **diff_refs**
> DiffList diff_refs(repository, left_ref, right_ref)

diff references

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import refs_api
from lakefs_client.model.diff_list import DiffList
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
    api_instance = refs_api.RefsApi(api_client)
    repository = "repository_example" # str | 
    left_ref = "leftRef_example" # str | a reference (could be either a branch or a commit ID)
    right_ref = "rightRef_example" # str | a reference (could be either a branch or a commit ID) to compare against
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    delimiter = "delimiter_example" # str | delimiter used to group common prefixes by (optional)
    type = "type_example" # str |  (optional)
    diff_type = "three_dot" # str |  (optional) if omitted the server will use the default value of "three_dot"

    # example passing only required values which don't have defaults set
    try:
        # diff references
        api_response = api_instance.diff_refs(repository, left_ref, right_ref)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->diff_refs: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # diff references
        api_response = api_instance.diff_refs(repository, left_ref, right_ref, after=after, amount=amount, prefix=prefix, delimiter=delimiter, type=type, diff_type=diff_type)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->diff_refs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **left_ref** | **str**| a reference (could be either a branch or a commit ID) |
 **right_ref** | **str**| a reference (could be either a branch or a commit ID) to compare against |
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100
 **prefix** | **str**| return items prefixed with this value | [optional]
 **delimiter** | **str**| delimiter used to group common prefixes by | [optional]
 **type** | **str**|  | [optional]
 **diff_type** | **str**|  | [optional] if omitted the server will use the default value of "three_dot"

### Return type

[**DiffList**](DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | diff between refs |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **dump_refs**
> RefsDump dump_refs(repository)

Dump repository refs (tags, commits, branches) to object store

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import refs_api
from lakefs_client.model.refs_dump import RefsDump
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
    api_instance = refs_api.RefsApi(api_client)
    repository = "repository_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # Dump repository refs (tags, commits, branches) to object store
        api_response = api_instance.dump_refs(repository)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->dump_refs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |

### Return type

[**RefsDump**](RefsDump.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | refs dump |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **log_commits**
> CommitList log_commits(repository, ref)

get commit log from ref. If both objects and prefixes are empty, return all commits.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import refs_api
from lakefs_client.model.commit_list import CommitList
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
    api_instance = refs_api.RefsApi(api_client)
    repository = "repository_example" # str | 
    ref = "ref_example" # str | 
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100
    objects = [
        "objects_example",
    ] # [str] | list of paths, each element is a path of a specific object (optional)
    prefixes = [
        "prefixes_example",
    ] # [str] | list of paths, each element is a path of a prefix (optional)
    limit = True # bool | limit the number of items in return to 'amount'. Without further indication on actual number of items. (optional)

    # example passing only required values which don't have defaults set
    try:
        # get commit log from ref. If both objects and prefixes are empty, return all commits.
        api_response = api_instance.log_commits(repository, ref)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->log_commits: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # get commit log from ref. If both objects and prefixes are empty, return all commits.
        api_response = api_instance.log_commits(repository, ref, after=after, amount=amount, objects=objects, prefixes=prefixes, limit=limit)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->log_commits: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**|  |
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100
 **objects** | **[str]**| list of paths, each element is a path of a specific object | [optional]
 **prefixes** | **[str]**| list of paths, each element is a path of a prefix | [optional]
 **limit** | **bool**| limit the number of items in return to &#39;amount&#39;. Without further indication on actual number of items. | [optional]

### Return type

[**CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

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

# **merge_into_branch**
> MergeResult merge_into_branch(repository, source_ref, destination_branch)

merge references

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import refs_api
from lakefs_client.model.merge import Merge
from lakefs_client.model.error import Error
from lakefs_client.model.merge_result import MergeResult
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
    api_instance = refs_api.RefsApi(api_client)
    repository = "repository_example" # str | 
    source_ref = "sourceRef_example" # str | source ref
    destination_branch = "destinationBranch_example" # str | destination branch name
    merge = Merge(
        message="message_example",
        metadata={
            "key": "key_example",
        },
        strategy="strategy_example",
    ) # Merge |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # merge references
        api_response = api_instance.merge_into_branch(repository, source_ref, destination_branch)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->merge_into_branch: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # merge references
        api_response = api_instance.merge_into_branch(repository, source_ref, destination_branch, merge=merge)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->merge_into_branch: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **source_ref** | **str**| source ref |
 **destination_branch** | **str**| destination branch name |
 **merge** | [**Merge**](Merge.md)|  | [optional]

### Return type

[**MergeResult**](MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | merge completed |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**409** | conflict |  -  |
**412** | precondition failed (e.g. a pre-merge hook returned a failure) |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **restore_refs**
> restore_refs(repository, refs_dump)

Restore repository refs (tags, commits, branches) from object store

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import refs_api
from lakefs_client.model.refs_dump import RefsDump
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
    api_instance = refs_api.RefsApi(api_client)
    repository = "repository_example" # str | 
    refs_dump = RefsDump(
        commits_meta_range_id="commits_meta_range_id_example",
        tags_meta_range_id="tags_meta_range_id_example",
        branches_meta_range_id="branches_meta_range_id_example",
    ) # RefsDump | 

    # example passing only required values which don't have defaults set
    try:
        # Restore repository refs (tags, commits, branches) from object store
        api_instance.restore_refs(repository, refs_dump)
    except lakefs_client.ApiException as e:
        print("Exception when calling RefsApi->restore_refs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **refs_dump** | [**RefsDump**](RefsDump.md)|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | refs successfully loaded |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

