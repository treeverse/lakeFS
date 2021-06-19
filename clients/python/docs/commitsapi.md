# CommitsApi

## lakefs\_client.CommitsApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**commit**](commitsapi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit |
| [**get\_commit**](commitsapi.md#get_commit) | **GET** /repositories/{repository}/commits/{commitId} | get commit |
| [**log\_branch\_commits**](commitsapi.md#log_branch_commits) | **GET** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref |

## **commit**

> Commit commit\(repository, branch, commit\_creation\)

create commit

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import commits\_api

  from lakefs\_client.model.commit import Commit

  from lakefs\_client.model.commit\_creation import CommitCreation

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
except lakefs_client.ApiException as e:
    print("Exception when calling CommitsApi->commit: %s\n" % e)
```

```text
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **commit_creation** | [**CommitCreation**](CommitCreation.md)|  |

### Return type

[**Commit**](Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

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
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import lakefs_client
from lakefs_client.api import commits_api
from lakefs_client.model.commit import Commit
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
    api_instance = commits_api.CommitsApi(api_client)
    repository = "repository_example" # str | 
    commit_id = "commitId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get commit
        api_response = api_instance.get_commit(repository, commit_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling CommitsApi->get_commit: %s\n" % e)
```

### Parameters

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |
| **commit\_id** | **str** |  |  |

### Return type

[**Commit**](commit.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| commit \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

[\[Back to top\]](commitsapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

## **log\_branch\_commits**

> CommitList log\_branch\_commits\(repository, branch\)

get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import commits\_api

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
except lakefs_client.ApiException as e:
    print("Exception when calling CommitsApi->log_branch_commits: %s\n" % e)

# example passing only required values which don't have defaults set
# and optional values
try:
    # get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
    api_response = api_instance.log_branch_commits(repository, branch, after=after, amount=amount)
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling CommitsApi->log_branch_commits: %s\n" % e)
```

\`\`\`

### Parameters

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |
| **branch** | **str** |  |  |
| **after** | **str** | return items after this value | \[optional\] |
| **amount** | **int** | how many items to return | \[optional\] if omitted the server will use the default value of 100 |

### Return type

[**CommitList**](commitlist.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| commit log \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

[\[Back to top\]](commitsapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

