<a name="__pageTop"></a>
# lakefs_client.apis.tags.staging_api.StagingApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_physical_address**](#get_physical_address) | **get** /repositories/{repository}/branches/{branch}/staging/backing | get a physical address and a return token to write object to underlying storage
[**link_physical_address**](#link_physical_address) | **put** /repositories/{repository}/branches/{branch}/staging/backing | associate staging on this physical address with a path

# **get_physical_address**
<a name="get_physical_address"></a>
> StagingLocation get_physical_address(repositorybranchpath)

get a physical address and a return token to write object to underlying storage

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import staging_api
from lakefs_client.model.staging_location import StagingLocation
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
    api_instance = staging_api.StagingApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
    }
    try:
        # get a physical address and a return token to write object to underlying storage
        api_response = api_instance.get_physical_address(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->get_physical_address: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
        'presign': True,
    }
    try:
        # get a physical address and a return token to write object to underlying storage
        api_response = api_instance.get_physical_address(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->get_physical_address: %s\n" % e)
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
path | PathSchema | | 
presign | PresignSchema | | optional


# PathSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# PresignSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bool,  | BoolClass,  |  | 

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
200 | [ApiResponseFor200](#get_physical_address.ApiResponseFor200) | physical address for staging area
401 | [ApiResponseFor401](#get_physical_address.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#get_physical_address.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#get_physical_address.ApiResponseForDefault) | Internal Server Error

#### get_physical_address.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**StagingLocation**](../../models/StagingLocation.md) |  | 


#### get_physical_address.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_physical_address.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_physical_address.ApiResponseForDefault
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

# **link_physical_address**
<a name="link_physical_address"></a>
> ObjectStats link_physical_address(repositorybranchpathstaging_metadata)

associate staging on this physical address with a path

If the supplied token matches the current staging token, associate the object as the physical address with the supplied path.  Otherwise, if staging has been committed and the token has expired, return a conflict and hint where to place the object to try again.  Caller should copy the object to the new physical address and PUT again with the new staging token.  (No need to back off, this is due to losing the race against a concurrent commit operation.) 

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import staging_api
from lakefs_client.model.staging_location import StagingLocation
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
from lakefs_client.model.staging_metadata import StagingMetadata
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
    api_instance = staging_api.StagingApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
    }
    body = StagingMetadata(
        staging=StagingLocation(
            physical_address="physical_address_example",
            token="token_example",
            presigned_url="presigned_url_example",
        ),
        checksum="checksum_example",
        size_bytes=1,
        user_metadata=dict(
            "key": "key_example",
        ),
        content_type="content_type_example",
    )
    try:
        # associate staging on this physical address with a path
        api_response = api_instance.link_physical_address(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling StagingApi->link_physical_address: %s\n" % e)
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
[**StagingMetadata**](../../models/StagingMetadata.md) |  | 


### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path | PathSchema | | 


# PathSchema

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
200 | [ApiResponseFor200](#link_physical_address.ApiResponseFor200) | object metadata
400 | [ApiResponseFor400](#link_physical_address.ApiResponseFor400) | Validation Error
401 | [ApiResponseFor401](#link_physical_address.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#link_physical_address.ApiResponseFor404) | Internal Server Error
409 | [ApiResponseFor409](#link_physical_address.ApiResponseFor409) | conflict with a commit, try here
default | [ApiResponseForDefault](#link_physical_address.ApiResponseForDefault) | Internal Server Error

#### link_physical_address.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectStats**](../../models/ObjectStats.md) |  | 


#### link_physical_address.ApiResponseFor400
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor400ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor400ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### link_physical_address.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### link_physical_address.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### link_physical_address.ApiResponseFor409
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor409ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor409ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**StagingLocation**](../../models/StagingLocation.md) |  | 


#### link_physical_address.ApiResponseForDefault
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

