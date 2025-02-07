# lakefs_sdk.RefsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**diff_refs**](RefsApi.md#diff_refs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
[**find_merge_base**](RefsApi.md#find_merge_base) | **GET** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | find the merge base for 2 references
[**log_commits**](RefsApi.md#log_commits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref. If both objects and prefixes are empty, return all commits.
[**merge_into_branch**](RefsApi.md#merge_into_branch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references


# **diff_refs**
> DiffList diff_refs(repository, left_ref, right_ref, after=after, amount=amount, prefix=prefix, delimiter=delimiter, type=type)

diff references

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
from lakefs_sdk.models.diff_list import DiffList
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
    api_instance = lakefs_sdk.RefsApi(api_client)
    repository = 'repository_example' # str | 
    left_ref = 'left_ref_example' # str | a reference (could be either a branch or a commit ID)
    right_ref = 'right_ref_example' # str | a reference (could be either a branch or a commit ID) to compare against
    after = 'after_example' # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) (default to 100)
    prefix = 'prefix_example' # str | return items prefixed with this value (optional)
    delimiter = 'delimiter_example' # str | delimiter used to group common prefixes by (optional)
    type = 'three_dot' # str |  (optional) (default to 'three_dot')

    try:
        # diff references
        api_response = api_instance.diff_refs(repository, left_ref, right_ref, after=after, amount=amount, prefix=prefix, delimiter=delimiter, type=type)
        print("The response of RefsApi->diff_refs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling RefsApi->diff_refs: %s\n" % e)
```


### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **left_ref** | **str**| a reference (could be either a branch or a commit ID) | 
 **right_ref** | **str**| a reference (could be either a branch or a commit ID) to compare against | 
 **after** | **str**| return items after this value | [optional] 
 **amount** | **int**| how many items to return | [optional] [default to 100]
 **prefix** | **str**| return items prefixed with this value | [optional] 
 **delimiter** | **str**| delimiter used to group common prefixes by | [optional] 
 **type** | **str**|  | [optional] [default to &#39;three_dot&#39;]

### Return type

[**DiffList**](DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | diff between refs |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **find_merge_base**
> FindMergeBaseResult find_merge_base(repository, source_ref, destination_branch)

find the merge base for 2 references

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
from lakefs_sdk.models.find_merge_base_result import FindMergeBaseResult
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
    api_instance = lakefs_sdk.RefsApi(api_client)
    repository = 'repository_example' # str | 
    source_ref = 'source_ref_example' # str | source ref
    destination_branch = 'destination_branch_example' # str | destination branch name

    try:
        # find the merge base for 2 references
        api_response = api_instance.find_merge_base(repository, source_ref, destination_branch)
        print("The response of RefsApi->find_merge_base:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling RefsApi->find_merge_base: %s\n" % e)
```


### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **source_ref** | **str**| source ref | 
 **destination_branch** | **str**| destination branch name | 

### Return type

[**FindMergeBaseResult**](FindMergeBaseResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Found the merge base |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **log_commits**
> CommitList log_commits(repository, ref, after=after, amount=amount, objects=objects, prefixes=prefixes, limit=limit, first_parent=first_parent, since=since, stop_at=stop_at)

get commit log from ref. If both objects and prefixes are empty, return all commits.

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
from lakefs_sdk.models.commit_list import CommitList
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
    api_instance = lakefs_sdk.RefsApi(api_client)
    repository = 'repository_example' # str | 
    ref = 'ref_example' # str | 
    after = 'after_example' # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) (default to 100)
    objects = ['objects_example'] # List[str] | list of paths, each element is a path of a specific object (optional)
    prefixes = ['prefixes_example'] # List[str] | list of paths, each element is a path of a prefix (optional)
    limit = True # bool | limit the number of items in return to 'amount'. Without further indication on actual number of items. (optional)
    first_parent = True # bool | if set to true, follow only the first parent upon reaching a merge commit (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime | Show commits more recent than a specific date-time. In case used with stop_at parameter, will stop at the first commit that meets any of the conditions. (optional)
    stop_at = 'stop_at_example' # str | A reference to stop at. In case used with since parameter, will stop at the first commit that meets any of the conditions. (optional)

    try:
        # get commit log from ref. If both objects and prefixes are empty, return all commits.
        api_response = api_instance.log_commits(repository, ref, after=after, amount=amount, objects=objects, prefixes=prefixes, limit=limit, first_parent=first_parent, since=since, stop_at=stop_at)
        print("The response of RefsApi->log_commits:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling RefsApi->log_commits: %s\n" % e)
```


### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **ref** | **str**|  | 
 **after** | **str**| return items after this value | [optional] 
 **amount** | **int**| how many items to return | [optional] [default to 100]
 **objects** | [**List[str]**](str.md)| list of paths, each element is a path of a specific object | [optional] 
 **prefixes** | [**List[str]**](str.md)| list of paths, each element is a path of a prefix | [optional] 
 **limit** | **bool**| limit the number of items in return to &#39;amount&#39;. Without further indication on actual number of items. | [optional] 
 **first_parent** | **bool**| if set to true, follow only the first parent upon reaching a merge commit | [optional] 
 **since** | **datetime**| Show commits more recent than a specific date-time. In case used with stop_at parameter, will stop at the first commit that meets any of the conditions. | [optional] 
 **stop_at** | **str**| A reference to stop at. In case used with since parameter, will stop at the first commit that meets any of the conditions. | [optional] 

### Return type

[**CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | commit log |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **merge_into_branch**
> MergeResult merge_into_branch(repository, source_ref, destination_branch, merge=merge)

merge references

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
from lakefs_sdk.models.merge import Merge
from lakefs_sdk.models.merge_result import MergeResult
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
    api_instance = lakefs_sdk.RefsApi(api_client)
    repository = 'repository_example' # str | 
    source_ref = 'source_ref_example' # str | source ref
    destination_branch = 'destination_branch_example' # str | destination branch name
    merge = lakefs_sdk.Merge() # Merge |  (optional)

    try:
        # merge references
        api_response = api_instance.merge_into_branch(repository, source_ref, destination_branch, merge=merge)
        print("The response of RefsApi->merge_into_branch:\n")
        pprint(api_response)
    except Exception as e:
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

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | merge completed |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**409** | Conflict Deprecated: content schema will return Error format and not an empty MergeResult  |  -  |
**412** | precondition failed (e.g. a pre-merge hook returned a failure) |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

