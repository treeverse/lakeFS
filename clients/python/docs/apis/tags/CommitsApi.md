<a name="__pageTop"></a>
# lakefs_client.apis.tags.commits_api.CommitsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**commit**](#commit) | **post** /repositories/{repository}/branches/{branch}/commits | create commit
[**get_commit**](#get_commit) | **get** /repositories/{repository}/commits/{commitId} | get commit
[**log_branch_commits**](#log_branch_commits) | **get** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 

# **commit**
<a name="commit"></a>
> Commit commit(repositorybranchcommit_creation)

create commit

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import commits_api
from lakefs_client.model.commit import Commit
from lakefs_client.model.commit_creation import CommitCreation
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "/api/v1"
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

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)
# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = commits_api.CommitsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
    }
    body = CommitCreation(
        message="message_example",
        metadata=dict(
            "key": "key_example",
        ),
        date=1,
    )
    try:
        # create commit
        api_response = api_instance.commit(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling CommitsApi->commit: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'source_metarange': "source_metarange_example",
    }
    body = CommitCreation(
        message="message_example",
        metadata=dict(
            "key": "key_example",
        ),
        date=1,
    )
    try:
        # create commit
        api_response = api_instance.commit(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling CommitsApi->commit: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CommitCreation**](../../models/CommitCreation.md) |  | 


### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
source_metarange | SourceMetarangeSchema | | optional


# SourceMetarangeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
repository | RepositorySchema | | 
branch | BranchSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# BranchSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
201 | [ApiResponseFor201](#commit.ApiResponseFor201) | commit
400 | [ApiResponseFor400](#commit.ApiResponseFor400) | Validation Error
401 | [ApiResponseFor401](#commit.ApiResponseFor401) | Unauthorized
403 | [ApiResponseFor403](#commit.ApiResponseFor403) | Forbidden
404 | [ApiResponseFor404](#commit.ApiResponseFor404) | Resource Not Found
412 | [ApiResponseFor412](#commit.ApiResponseFor412) | Precondition Failed (e.g. a pre-commit hook returned a failure)
default | [ApiResponseForDefault](#commit.ApiResponseForDefault) | Internal Server Error

#### commit.ApiResponseFor201
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor201ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor201ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Commit**](../../models/Commit.md) |  | 


#### commit.ApiResponseFor400
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor400ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor400ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### commit.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### commit.ApiResponseFor403
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor403ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor403ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### commit.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### commit.ApiResponseFor412
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor412ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor412ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### commit.ApiResponseForDefault
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor0ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor0ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


### Authorization

[basic_auth](../../../README.md#basic_auth), [cookie_auth](../../../README.md#cookie_auth), [oidc_auth](../../../README.md#oidc_auth), [saml_auth](../../../README.md#saml_auth), [jwt_token](../../../README.md#jwt_token)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **get_commit**
<a name="get_commit"></a>
> Commit get_commit(repositorycommit_id)

get commit

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import commits_api
from lakefs_client.model.commit import Commit
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "/api/v1"
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

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)
# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = commits_api.CommitsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'commitId': "commitId_example",
    }
    try:
        # get commit
        api_response = api_instance.get_commit(
            path_params=path_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling CommitsApi->get_commit: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
repository | RepositorySchema | | 
commitId | CommitIdSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# CommitIdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#get_commit.ApiResponseFor200) | commit
401 | [ApiResponseFor401](#get_commit.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#get_commit.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#get_commit.ApiResponseForDefault) | Internal Server Error

#### get_commit.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Commit**](../../models/Commit.md) |  | 


#### get_commit.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_commit.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_commit.ApiResponseForDefault
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor0ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor0ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


### Authorization

[basic_auth](../../../README.md#basic_auth), [cookie_auth](../../../README.md#cookie_auth), [oidc_auth](../../../README.md#oidc_auth), [saml_auth](../../../README.md#saml_auth), [jwt_token](../../../README.md#jwt_token)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **log_branch_commits**
<a name="log_branch_commits"></a>
> CommitList log_branch_commits(repositorybranch)

get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import commits_api
from lakefs_client.model.commit_list import CommitList
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "/api/v1"
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

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)
# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = commits_api.CommitsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
    }
    try:
        # get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
        api_response = api_instance.log_branch_commits(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling CommitsApi->log_branch_commits: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'after': "after_example",
        'amount': 100,
    }
    try:
        # get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
        api_response = api_instance.log_branch_commits(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling CommitsApi->log_branch_commits: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
after | AfterSchema | | optional
amount | AmountSchema | | optional


# AfterSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# AmountSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | if omitted the server will use the default value of 100

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
repository | RepositorySchema | | 
branch | BranchSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# BranchSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#log_branch_commits.ApiResponseFor200) | commit log
401 | [ApiResponseFor401](#log_branch_commits.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#log_branch_commits.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#log_branch_commits.ApiResponseForDefault) | Internal Server Error

#### log_branch_commits.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CommitList**](../../models/CommitList.md) |  | 


#### log_branch_commits.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### log_branch_commits.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### log_branch_commits.ApiResponseForDefault
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor0ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor0ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


### Authorization

[basic_auth](../../../README.md#basic_auth), [cookie_auth](../../../README.md#cookie_auth), [oidc_auth](../../../README.md#oidc_auth), [saml_auth](../../../README.md#saml_auth), [jwt_token](../../../README.md#jwt_token)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

