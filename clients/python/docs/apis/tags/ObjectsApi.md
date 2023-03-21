<a name="__pageTop"></a>
# lakefs_client.apis.tags.objects_api.ObjectsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**copy_object**](#copy_object) | **post** /repositories/{repository}/branches/{branch}/objects/copy | create a copy of an object
[**delete_object**](#delete_object) | **delete** /repositories/{repository}/branches/{branch}/objects | delete object. Missing objects will not return a NotFound error.
[**delete_objects**](#delete_objects) | **post** /repositories/{repository}/branches/{branch}/objects/delete | delete objects. Missing objects will not return a NotFound error.
[**get_object**](#get_object) | **get** /repositories/{repository}/refs/{ref}/objects | get object content
[**get_underlying_properties**](#get_underlying_properties) | **get** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage
[**head_object**](#head_object) | **head** /repositories/{repository}/refs/{ref}/objects | check if object exists
[**list_objects**](#list_objects) | **get** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix
[**stage_object**](#stage_object) | **put** /repositories/{repository}/branches/{branch}/objects | stage an object&#x27;s metadata for the given branch
[**stat_object**](#stat_object) | **get** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata
[**upload_object**](#upload_object) | **post** /repositories/{repository}/branches/{branch}/objects | 

# **copy_object**
<a name="copy_object"></a>
> ObjectStats copy_object(repositorybranchdest_pathobject_copy_creation)

create a copy of an object

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.object_copy_creation import ObjectCopyCreation
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'dest_path': "dest_path_example",
    }
    body = ObjectCopyCreation(
        src_path="src_path_example",
        src_ref="src_ref_example",
    )
    try:
        # create a copy of an object
        api_response = api_instance.copy_object(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->copy_object: %s\n" % e)
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
[**ObjectCopyCreation**](../../models/ObjectCopyCreation.md) |  | 


### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
dest_path | DestPathSchema | | 


# DestPathSchema

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
201 | [ApiResponseFor201](#copy_object.ApiResponseFor201) | Copy object response
400 | [ApiResponseFor400](#copy_object.ApiResponseFor400) | Validation Error
401 | [ApiResponseFor401](#copy_object.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#copy_object.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#copy_object.ApiResponseForDefault) | Internal Server Error

#### copy_object.ApiResponseFor201
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor201ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor201ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectStats**](../../models/ObjectStats.md) |  | 


#### copy_object.ApiResponseFor400
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor400ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor400ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### copy_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### copy_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### copy_object.ApiResponseForDefault
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

# **delete_object**
<a name="delete_object"></a>
> delete_object(repositorybranchpath)

delete object. Missing objects will not return a NotFound error.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
    }
    try:
        # delete object. Missing objects will not return a NotFound error.
        api_response = api_instance.delete_object(
            path_params=path_params,
            query_params=query_params,
        )
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->delete_object: %s\n" % e)
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
204 | [ApiResponseFor204](#delete_object.ApiResponseFor204) | object deleted successfully
401 | [ApiResponseFor401](#delete_object.ApiResponseFor401) | Unauthorized
403 | [ApiResponseFor403](#delete_object.ApiResponseFor403) | Forbidden
404 | [ApiResponseFor404](#delete_object.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#delete_object.ApiResponseForDefault) | Internal Server Error

#### delete_object.ApiResponseFor204
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

#### delete_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### delete_object.ApiResponseFor403
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor403ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor403ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### delete_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### delete_object.ApiResponseForDefault
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

# **delete_objects**
<a name="delete_objects"></a>
> ObjectErrorList delete_objects(repositorybranchpath_list)

delete objects. Missing objects will not return a NotFound error.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.object_error_list import ObjectErrorList
from lakefs_client.model.error import Error
from lakefs_client.model.path_list import PathList
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    body = PathList(
        paths=[
            "paths_example"
        ],
    )
    try:
        # delete objects. Missing objects will not return a NotFound error.
        api_response = api_instance.delete_objects(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->delete_objects: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
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
[**PathList**](../../models/PathList.md) |  | 


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
200 | [ApiResponseFor200](#delete_objects.ApiResponseFor200) | Delete objects response
401 | [ApiResponseFor401](#delete_objects.ApiResponseFor401) | Unauthorized
403 | [ApiResponseFor403](#delete_objects.ApiResponseFor403) | Forbidden
404 | [ApiResponseFor404](#delete_objects.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#delete_objects.ApiResponseForDefault) | Internal Server Error

#### delete_objects.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectErrorList**](../../models/ObjectErrorList.md) |  | 


#### delete_objects.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### delete_objects.ApiResponseFor403
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor403ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor403ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### delete_objects.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### delete_objects.ApiResponseForDefault
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

# **get_object**
<a name="get_object"></a>
> file_type get_object(repositoryrefpath)

get object content

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
    }
    header_params = {
    }
    try:
        # get object content
        api_response = api_instance.get_object(
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->get_object: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
        'presign': True,
    }
    header_params = {
        'Range': "bytes=0-1023",
    }
    try:
        # get object content
        api_response = api_instance.get_object(
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->get_object: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
header_params | RequestHeaderParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/octet-stream', 'application/json', ) | Tells the server the content type(s) that are accepted by the client
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

### header_params
#### RequestHeaderParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Range | RangeSchema | | optional

# RangeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
repository | RepositorySchema | | 
ref | RefSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# RefSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#get_object.ApiResponseFor200) | object content
206 | [ApiResponseFor206](#get_object.ApiResponseFor206) | partial object content
302 | [ApiResponseFor302](#get_object.ApiResponseFor302) | Redirect to a pre-signed URL for the object
401 | [ApiResponseFor401](#get_object.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#get_object.ApiResponseFor404) | Resource Not Found
410 | [ApiResponseFor410](#get_object.ApiResponseFor410) | object expired
416 | [ApiResponseFor416](#get_object.ApiResponseFor416) | Requested Range Not Satisfiable
default | [ApiResponseForDefault](#get_object.ApiResponseForDefault) | Internal Server Error

#### get_object.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationOctetStream, ] |  |
headers | ResponseHeadersFor200 |  |

# SchemaFor200ResponseBodyApplicationOctetStream

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bytes, io.FileIO, io.BufferedReader,  | bytes, FileIO,  |  | 
#### ResponseHeadersFor200

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Content-Length | ContentLengthSchema | | optional
Last-Modified | LastModifiedSchema | | optional
ETag | ETagSchema | | optional

# ContentLengthSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

# LastModifiedSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# ETagSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 


#### get_object.ApiResponseFor206
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor206ResponseBodyApplicationOctetStream, ] |  |
headers | ResponseHeadersFor206 |  |

# SchemaFor206ResponseBodyApplicationOctetStream

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bytes, io.FileIO, io.BufferedReader,  | bytes, FileIO,  |  | 
#### ResponseHeadersFor206

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Content-Length | ContentLengthSchema | | optional
Content-Range | ContentRangeSchema | | optional
Last-Modified | LastModifiedSchema | | optional
ETag | ETagSchema | | optional

# ContentLengthSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

# ContentRangeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# LastModifiedSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# ETagSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 


#### get_object.ApiResponseFor302
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | ResponseHeadersFor302 |  |
#### ResponseHeadersFor302

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Location | LocationSchema | | optional

# LocationSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 


#### get_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_object.ApiResponseFor410
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor410ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor410ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_object.ApiResponseFor416
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor416ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor416ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_object.ApiResponseForDefault
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

# **get_underlying_properties**
<a name="get_underlying_properties"></a>
> UnderlyingObjectProperties get_underlying_properties(repositoryrefpath)

get object properties on underlying storage

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.underlying_object_properties import UnderlyingObjectProperties
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
    }
    try:
        # get object properties on underlying storage
        api_response = api_instance.get_underlying_properties(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->get_underlying_properties: %s\n" % e)
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
ref | RefSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# RefSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#get_underlying_properties.ApiResponseFor200) | object metadata on underlying storage
401 | [ApiResponseFor401](#get_underlying_properties.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#get_underlying_properties.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#get_underlying_properties.ApiResponseForDefault) | Internal Server Error

#### get_underlying_properties.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**UnderlyingObjectProperties**](../../models/UnderlyingObjectProperties.md) |  | 


#### get_underlying_properties.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_underlying_properties.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### get_underlying_properties.ApiResponseForDefault
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

# **head_object**
<a name="head_object"></a>
> head_object(repositoryrefpath)

check if object exists

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
    }
    header_params = {
    }
    try:
        # check if object exists
        api_response = api_instance.head_object(
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
        )
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->head_object: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
    }
    header_params = {
        'Range': "bytes=0-1023",
    }
    try:
        # check if object exists
        api_response = api_instance.head_object(
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
        )
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->head_object: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
header_params | RequestHeaderParams | |
path_params | RequestPathParams | |
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

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

### header_params
#### RequestHeaderParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Range | RangeSchema | | optional

# RangeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
repository | RepositorySchema | | 
ref | RefSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# RefSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#head_object.ApiResponseFor200) | object exists
206 | [ApiResponseFor206](#head_object.ApiResponseFor206) | partial object content info
401 | [ApiResponseFor401](#head_object.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#head_object.ApiResponseFor404) | object not found
410 | [ApiResponseFor410](#head_object.ApiResponseFor410) | object expired
416 | [ApiResponseFor416](#head_object.ApiResponseFor416) | Requested Range Not Satisfiable
default | [ApiResponseForDefault](#head_object.ApiResponseForDefault) | internal server error

#### head_object.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | ResponseHeadersFor200 |  |
#### ResponseHeadersFor200

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Content-Length | ContentLengthSchema | | optional
Last-Modified | LastModifiedSchema | | optional
ETag | ETagSchema | | optional

# ContentLengthSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

# LastModifiedSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# ETagSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 


#### head_object.ApiResponseFor206
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | ResponseHeadersFor206 |  |
#### ResponseHeadersFor206

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Content-Length | ContentLengthSchema | | optional
Content-Range | ContentRangeSchema | | optional
Last-Modified | LastModifiedSchema | | optional
ETag | ETagSchema | | optional

# ContentLengthSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

# ContentRangeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# LastModifiedSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# ETagSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 


#### head_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

#### head_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

#### head_object.ApiResponseFor410
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

#### head_object.ApiResponseFor416
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

#### head_object.ApiResponseForDefault
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

### Authorization

[basic_auth](../../../README.md#basic_auth), [cookie_auth](../../../README.md#cookie_auth), [oidc_auth](../../../README.md#oidc_auth), [saml_auth](../../../README.md#saml_auth), [jwt_token](../../../README.md#jwt_token)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **list_objects**
<a name="list_objects"></a>
> ObjectStatsList list_objects(repositoryref)

list objects under a given prefix

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.object_stats_list import ObjectStatsList
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
    }
    try:
        # list objects under a given prefix
        api_response = api_instance.list_objects(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->list_objects: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'user_metadata': True,
        'presign': True,
        'after': "after_example",
        'amount': 100,
        'delimiter': "delimiter_example",
        'prefix': "prefix_example",
    }
    try:
        # list objects under a given prefix
        api_response = api_instance.list_objects(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->list_objects: %s\n" % e)
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
user_metadata | UserMetadataSchema | | optional
presign | PresignSchema | | optional
after | AfterSchema | | optional
amount | AmountSchema | | optional
delimiter | DelimiterSchema | | optional
prefix | PrefixSchema | | optional


# UserMetadataSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bool,  | BoolClass,  |  | if omitted the server will use the default value of True

# PresignSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bool,  | BoolClass,  |  | 

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

# DelimiterSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# PrefixSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
repository | RepositorySchema | | 
ref | RefSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# RefSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_objects.ApiResponseFor200) | object listing
401 | [ApiResponseFor401](#list_objects.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#list_objects.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#list_objects.ApiResponseForDefault) | Internal Server Error

#### list_objects.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectStatsList**](../../models/ObjectStatsList.md) |  | 


#### list_objects.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### list_objects.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### list_objects.ApiResponseForDefault
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

# **stage_object**
<a name="stage_object"></a>
> ObjectStats stage_object(repositorybranchpathobject_stage_creation)

stage an object's metadata for the given branch

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.object_stage_creation import ObjectStageCreation
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
    }
    body = ObjectStageCreation(
        physical_address="physical_address_example",
        checksum="checksum_example",
        size_bytes=1,
        mtime=1,
        metadata=ObjectUserMetadata(
            key="key_example",
        ),
        content_type="content_type_example",
    )
    try:
        # stage an object's metadata for the given branch
        api_response = api_instance.stage_object(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->stage_object: %s\n" % e)
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
[**ObjectStageCreation**](../../models/ObjectStageCreation.md) |  | 


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
201 | [ApiResponseFor201](#stage_object.ApiResponseFor201) | object metadata
400 | [ApiResponseFor400](#stage_object.ApiResponseFor400) | Validation Error
401 | [ApiResponseFor401](#stage_object.ApiResponseFor401) | Unauthorized
403 | [ApiResponseFor403](#stage_object.ApiResponseFor403) | Forbidden
404 | [ApiResponseFor404](#stage_object.ApiResponseFor404) | Resource Not Found
default | [ApiResponseForDefault](#stage_object.ApiResponseForDefault) | Internal Server Error

#### stage_object.ApiResponseFor201
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor201ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor201ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectStats**](../../models/ObjectStats.md) |  | 


#### stage_object.ApiResponseFor400
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor400ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor400ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### stage_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### stage_object.ApiResponseFor403
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor403ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor403ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### stage_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### stage_object.ApiResponseForDefault
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

# **stat_object**
<a name="stat_object"></a>
> ObjectStats stat_object(repositoryrefpath)

get object metadata

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
    }
    try:
        # get object metadata
        api_response = api_instance.stat_object(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->stat_object: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'ref': "ref_example",
    }
    query_params = {
        'path': "path_example",
        'user_metadata': True,
        'presign': True,
    }
    try:
        # get object metadata
        api_response = api_instance.stat_object(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->stat_object: %s\n" % e)
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
user_metadata | UserMetadataSchema | | optional
presign | PresignSchema | | optional


# PathSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# UserMetadataSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bool,  | BoolClass,  |  | if omitted the server will use the default value of True

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
ref | RefSchema | | 

# RepositorySchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# RefSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#stat_object.ApiResponseFor200) | object metadata
401 | [ApiResponseFor401](#stat_object.ApiResponseFor401) | Unauthorized
404 | [ApiResponseFor404](#stat_object.ApiResponseFor404) | Resource Not Found
410 | [ApiResponseFor410](#stat_object.ApiResponseFor410) | object gone (but partial metadata may be available)
default | [ApiResponseForDefault](#stat_object.ApiResponseForDefault) | Internal Server Error

#### stat_object.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectStats**](../../models/ObjectStats.md) |  | 


#### stat_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### stat_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### stat_object.ApiResponseFor410
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

#### stat_object.ApiResponseForDefault
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

# **upload_object**
<a name="upload_object"></a>
> ObjectStats upload_object(repositorybranchpath)



### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import lakefs_client
from lakefs_client.apis.tags import objects_api
from lakefs_client.model.error import Error
from lakefs_client.model.object_stats import ObjectStats
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
    api_instance = objects_api.ObjectsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
    }
    header_params = {
    }
    try:
        api_response = api_instance.upload_object(
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->upload_object: %s\n" % e)

    # example passing only optional values
    path_params = {
        'repository': "repository_example",
        'branch': "branch_example",
    }
    query_params = {
        'path': "path_example",
        'storageClass': "storageClass_example",
    }
    header_params = {
        'If-None-Match': "*",
    }
    body = dict(
        content=open('/path/to/file', 'rb'),
    )
    try:
        api_response = api_instance.upload_object(
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
            body=body,
        )
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ObjectsApi->upload_object: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyMultipartFormData, Unset] | optional, default is unset |
query_params | RequestQueryParams | |
header_params | RequestHeaderParams | |
path_params | RequestPathParams | |
content_type | str | optional, default is 'multipart/form-data' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyMultipartFormData

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**content** | bytes, io.FileIO, io.BufferedReader,  | bytes, FileIO,  | Only a single file per upload which must be named \&quot;content\&quot;. | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path | PathSchema | | 
storageClass | StorageClassSchema | | optional


# PathSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# StorageClassSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### header_params
#### RequestHeaderParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
If-None-Match | IfNoneMatchSchema | | optional

# IfNoneMatchSchema

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
201 | [ApiResponseFor201](#upload_object.ApiResponseFor201) | object metadata
400 | [ApiResponseFor400](#upload_object.ApiResponseFor400) | Validation Error
401 | [ApiResponseFor401](#upload_object.ApiResponseFor401) | Unauthorized
403 | [ApiResponseFor403](#upload_object.ApiResponseFor403) | Forbidden
404 | [ApiResponseFor404](#upload_object.ApiResponseFor404) | Resource Not Found
412 | [ApiResponseFor412](#upload_object.ApiResponseFor412) | Precondition Failed
default | [ApiResponseForDefault](#upload_object.ApiResponseForDefault) | Internal Server Error

#### upload_object.ApiResponseFor201
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor201ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor201ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ObjectStats**](../../models/ObjectStats.md) |  | 


#### upload_object.ApiResponseFor400
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor400ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor400ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### upload_object.ApiResponseFor401
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor401ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor401ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### upload_object.ApiResponseFor403
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor403ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor403ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### upload_object.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor404ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor404ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### upload_object.ApiResponseFor412
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor412ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor412ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Error**](../../models/Error.md) |  | 


#### upload_object.ApiResponseForDefault
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

