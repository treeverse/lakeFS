# RefsApi

## lakefs\_client.RefsApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**diff\_refs**](refsapi.md#diff_refs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references |
| [**dump\_refs**](refsapi.md#dump_refs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs \(tags, commits, branches\) to object store |
| [**log\_commits**](refsapi.md#log_commits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref |
| [**merge\_into\_branch**](refsapi.md#merge_into_branch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references |
| [**restore\_refs**](refsapi.md#restore_refs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs \(tags, commits, branches\) from object store |

## **diff\_refs**

> DiffList diff\_refs\(repository, left\_ref, right\_ref\)

diff references

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import refs\_api

  from lakefs\_client.model.diff\_list import DiffList

  from lakefs\_client.model.error import Error

  from pprint import pprint

  **Defining the host is optional and defaults to** [**http://localhost/api/v1**](http://localhost/api/v1)\*\*\*\*

  **See configuration.py for a list of all supported configuration parameters.**

  configuration = lakefs\_client.Configuration\(

    host = "[http://localhost/api/v1](http://localhost/api/v1)"

  \)

## The client must configure the authentication and authorization parameters

## in accordance with the API server security policy.

## Examples for each auth method are provided below, use the example that

## satisfies your auth use case.

## Configure HTTP basic authorization: basic\_auth

configuration = lakefs\_client.Configuration\( username = 'YOUR\_USERNAME', password = 'YOUR\_PASSWORD' \)

## Configure API key authorization: cookie\_auth

configuration.api\_key\['cookie\_auth'\] = 'YOUR\_API\_KEY'

## Uncomment below to setup prefix \(e.g. Bearer\) for API key, if needed

## configuration.api\_key\_prefix\['cookie\_auth'\] = 'Bearer'

## Configure Bearer authorization \(JWT\): jwt\_token

configuration = lakefs\_client.Configuration\( access\_token = 'YOUR\_BEARER\_TOKEN' \)

## Enter a context with an instance of the API client

with lakefs\_client.ApiClient\(configuration\) as api\_client:

```text
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

```text
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

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |

### Return type

[**RefsDump**](refsdump.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**201** \| refs dump \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

[\[Back to top\]](refsapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

## **log\_commits**

> CommitList log\_commits\(repository, ref\)

get commit log from ref

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import refs\_api

  from lakefs\_client.model.commit\_list import CommitList

  from lakefs\_client.model.error import Error

  from pprint import pprint

  **Defining the host is optional and defaults to** [**http://localhost/api/v1**](http://localhost/api/v1)\*\*\*\*

  **See configuration.py for a list of all supported configuration parameters.**

  configuration = lakefs\_client.Configuration\(

    host = "[http://localhost/api/v1](http://localhost/api/v1)"

  \)

## The client must configure the authentication and authorization parameters

## in accordance with the API server security policy.

## Examples for each auth method are provided below, use the example that

## satisfies your auth use case.

## Configure HTTP basic authorization: basic\_auth

configuration = lakefs\_client.Configuration\( username = 'YOUR\_USERNAME', password = 'YOUR\_PASSWORD' \)

## Configure API key authorization: cookie\_auth

configuration.api\_key\['cookie\_auth'\] = 'YOUR\_API\_KEY'

## Uncomment below to setup prefix \(e.g. Bearer\) for API key, if needed

## configuration.api\_key\_prefix\['cookie\_auth'\] = 'Bearer'

## Configure Bearer authorization \(JWT\): jwt\_token

configuration = lakefs\_client.Configuration\( access\_token = 'YOUR\_BEARER\_TOKEN' \)

## Enter a context with an instance of the API client

with lakefs\_client.ApiClient\(configuration\) as api\_client:

```text
# Create an instance of the API class
api_instance = refs_api.RefsApi(api_client)
repository = "repository_example" # str | 
ref = "ref_example" # str | 
after = "after_example" # str | return items after this value (optional)
amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

# example passing only required values which don't have defaults set
try:
    # get commit log from ref
    api_response = api_instance.log_commits(repository, ref)
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling RefsApi->log_commits: %s\n" % e)

# example passing only required values which don't have defaults set
# and optional values
try:
    # get commit log from ref
    api_response = api_instance.log_commits(repository, ref, after=after, amount=amount)
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling RefsApi->log_commits: %s\n" % e)
```

```text
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **ref** | **str**|  |
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |
| **source\_ref** | **str** | source ref |  |
| **destination\_branch** | **str** | destination branch name |  |
| **merge** | [**Merge**](merge.md) |  | \[optional\] |

### Return type

[**MergeResult**](mergeresult.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| merge completed \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **409** \| conflict \| - \| **412** \| precondition failed \(e.g. a pre-merge hook returned a failure\) \| - \| **0** \| Internal Server Error \| - \|

[\[Back to top\]](refsapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

## **restore\_refs**

> restore\_refs\(repository, refs\_dump\)

Restore repository refs \(tags, commits, branches\) from object store

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import refs\_api

  from lakefs\_client.model.refs\_dump import RefsDump

  from lakefs\_client.model.error import Error

  from pprint import pprint

  **Defining the host is optional and defaults to** [**http://localhost/api/v1**](http://localhost/api/v1)\*\*\*\*

  **See configuration.py for a list of all supported configuration parameters.**

  configuration = lakefs\_client.Configuration\(

    host = "[http://localhost/api/v1](http://localhost/api/v1)"

  \)

## The client must configure the authentication and authorization parameters

## in accordance with the API server security policy.

## Examples for each auth method are provided below, use the example that

## satisfies your auth use case.

## Configure HTTP basic authorization: basic\_auth

configuration = lakefs\_client.Configuration\( username = 'YOUR\_USERNAME', password = 'YOUR\_PASSWORD' \)

## Configure API key authorization: cookie\_auth

configuration.api\_key\['cookie\_auth'\] = 'YOUR\_API\_KEY'

## Uncomment below to setup prefix \(e.g. Bearer\) for API key, if needed

## configuration.api\_key\_prefix\['cookie\_auth'\] = 'Bearer'

## Configure Bearer authorization \(JWT\): jwt\_token

configuration = lakefs\_client.Configuration\( access\_token = 'YOUR\_BEARER\_TOKEN' \)

## Enter a context with an instance of the API client

with lakefs\_client.ApiClient\(configuration\) as api\_client:

```text
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

\`\`\`

### Parameters

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |
| **refs\_dump** | [**RefsDump**](refsdump.md) |  |  |

### Return type

void \(empty response body\)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| refs successfully loaded \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

[\[Back to top\]](refsapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

