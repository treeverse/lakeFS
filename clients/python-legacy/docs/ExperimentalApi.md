# lakefs_client.ExperimentalApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**abort_presign_multipart_upload**](ExperimentalApi.md#abort_presign_multipart_upload) | **DELETE** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Abort a presign multipart upload
[**complete_presign_multipart_upload**](ExperimentalApi.md#complete_presign_multipart_upload) | **PUT** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Complete a presign multipart upload request
[**create_presign_multipart_upload**](ExperimentalApi.md#create_presign_multipart_upload) | **POST** /repositories/{repository}/branches/{branch}/staging/pmpu | Initiate a multipart upload
[**create_user_external_principal**](ExperimentalApi.md#create_user_external_principal) | **POST** /auth/users/{userId}/external/principal/{principalId} | attach external principal to user
[**delete_user_external_principal**](ExperimentalApi.md#delete_user_external_principal) | **DELETE** /auth/users/{userId}/external/principal/{principalId} | delete external principal from user
[**get_user_external_principal**](ExperimentalApi.md#get_user_external_principal) | **GET** /auth/users/{userId}/external/principal/{principalId} | get external principal of a user
[**hard_reset_branch**](ExperimentalApi.md#hard_reset_branch) | **PUT** /repositories/{repository}/branches/{branch}/hard_reset | hard reset branch
[**list_user_external_principals**](ExperimentalApi.md#list_user_external_principals) | **GET** /auth/users/{userId}/external/principals | list user external policies attached to a user


# **abort_presign_multipart_upload**
> abort_presign_multipart_upload(repository, branch, upload_id, path)

Abort a presign multipart upload

Aborts a presign multipart upload.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.abort_presign_multipart_upload import AbortPresignMultipartUpload
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    upload_id = "uploadId_example" # str | 
    path = "path_example" # str | relative to the branch
    abort_presign_multipart_upload = AbortPresignMultipartUpload(
        physical_address="physical_address_example",
    ) # AbortPresignMultipartUpload |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Abort a presign multipart upload
        api_instance.abort_presign_multipart_upload(repository, branch, upload_id, path)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->abort_presign_multipart_upload: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Abort a presign multipart upload
        api_instance.abort_presign_multipart_upload(repository, branch, upload_id, path, abort_presign_multipart_upload=abort_presign_multipart_upload)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->abort_presign_multipart_upload: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **upload_id** | **str**|  |
 **path** | **str**| relative to the branch |
 **abort_presign_multipart_upload** | [**AbortPresignMultipartUpload**](AbortPresignMultipartUpload.md)|  | [optional]

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Presign multipart upload aborted |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **complete_presign_multipart_upload**
> ObjectStats complete_presign_multipart_upload(repository, branch, upload_id, path)

Complete a presign multipart upload request

Completes a presign multipart upload by assembling the uploaded parts.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.staging_location import StagingLocation
from lakefs_client.model.complete_presign_multipart_upload import CompletePresignMultipartUpload
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    upload_id = "uploadId_example" # str | 
    path = "path_example" # str | relative to the branch
    complete_presign_multipart_upload = CompletePresignMultipartUpload(
        physical_address="physical_address_example",
        parts=[
            UploadPart(
                part_number=1,
                etag="etag_example",
            ),
        ],
        user_metadata={
            "key": "key_example",
        },
        content_type="content_type_example",
    ) # CompletePresignMultipartUpload |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Complete a presign multipart upload request
        api_response = api_instance.complete_presign_multipart_upload(repository, branch, upload_id, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->complete_presign_multipart_upload: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Complete a presign multipart upload request
        api_response = api_instance.complete_presign_multipart_upload(repository, branch, upload_id, path, complete_presign_multipart_upload=complete_presign_multipart_upload)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->complete_presign_multipart_upload: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **upload_id** | **str**|  |
 **path** | **str**| relative to the branch |
 **complete_presign_multipart_upload** | [**CompletePresignMultipartUpload**](CompletePresignMultipartUpload.md)|  | [optional]

### Return type

[**ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Presign multipart upload completed |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**409** | conflict with a commit, try here |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_presign_multipart_upload**
> PresignMultipartUpload create_presign_multipart_upload(repository, branch, path)

Initiate a multipart upload

Initiates a multipart upload and returns an upload ID with presigned URLs for each part (optional). Part numbers starts with 1. Each part except the last one has minimum size depends on the underlying blockstore implementation. For example working with S3 blockstore, minimum size is 5MB (excluding the last part). 

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.presign_multipart_upload import PresignMultipartUpload
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    path = "path_example" # str | relative to the branch
    parts = 1 # int | number of presigned URL parts required to upload (optional)

    # example passing only required values which don't have defaults set
    try:
        # Initiate a multipart upload
        api_response = api_instance.create_presign_multipart_upload(repository, branch, path)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->create_presign_multipart_upload: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Initiate a multipart upload
        api_response = api_instance.create_presign_multipart_upload(repository, branch, path, parts=parts)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->create_presign_multipart_upload: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **path** | **str**| relative to the branch |
 **parts** | **int**| number of presigned URL parts required to upload | [optional]

### Return type

[**PresignMultipartUpload**](PresignMultipartUpload.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Presign multipart upload initiated |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_user_external_principal**
> create_user_external_principal(user_id, principal_id, external_principal_creation)

attach external principal to user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.error import Error
from lakefs_client.model.external_principal_creation import ExternalPrincipalCreation
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    user_id = "userId_example" # str | 
    principal_id = "principalId_example" # str | 
    external_principal_creation = ExternalPrincipalCreation(
        settings=[
            ExternalPrincipalSettings(
                key="key_example",
            ),
        ],
    ) # ExternalPrincipalCreation | 

    # example passing only required values which don't have defaults set
    try:
        # attach external principal to user
        api_instance.create_user_external_principal(user_id, principal_id, external_principal_creation)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->create_user_external_principal: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **principal_id** | **str**|  |
 **external_principal_creation** | [**ExternalPrincipalCreation**](ExternalPrincipalCreation.md)|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | external principal attached successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**409** | Resource Conflicts With Target |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_user_external_principal**
> delete_user_external_principal(user_id, principal_id)

delete external principal from user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    user_id = "userId_example" # str | 
    principal_id = "principalId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # delete external principal from user
        api_instance.delete_user_external_principal(user_id, principal_id)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->delete_user_external_principal: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **principal_id** | **str**|  |

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | external principal detached successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_user_external_principal**
> ExternalPrincipal get_user_external_principal(user_id, principal_id)

get external principal of a user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.external_principal import ExternalPrincipal
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    user_id = "userId_example" # str | 
    principal_id = "principalId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # get external principal of a user
        api_response = api_instance.get_user_external_principal(user_id, principal_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->get_user_external_principal: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **principal_id** | **str**|  |

### Return type

[**ExternalPrincipal**](ExternalPrincipal.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | external principal |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **hard_reset_branch**
> hard_reset_branch(repository, branch, ref)

hard reset branch

Relocate branch to refer to ref.  Branch must not contain uncommitted data.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    repository = "repository_example" # str | 
    branch = "branch_example" # str | 
    ref = "ref_example" # str | After reset, branch will point at this reference.
    force = False # bool |  (optional) if omitted the server will use the default value of False

    # example passing only required values which don't have defaults set
    try:
        # hard reset branch
        api_instance.hard_reset_branch(repository, branch, ref)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->hard_reset_branch: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # hard reset branch
        api_instance.hard_reset_branch(repository, branch, ref, force=force)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->hard_reset_branch: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **ref** | **str**| After reset, branch will point at this reference. |
 **force** | **bool**|  | [optional] if omitted the server will use the default value of False

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | reset successful |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_user_external_principals**
> ExternalPrincipalList list_user_external_principals(user_id)

list user external policies attached to a user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import experimental_api
from lakefs_client.model.error import Error
from lakefs_client.model.external_principal_list import ExternalPrincipalList
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

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = experimental_api.ExperimentalApi(api_client)
    user_id = "userId_example" # str | 
    prefix = "prefix_example" # str | return items prefixed with this value (optional)
    after = "after_example" # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) if omitted the server will use the default value of 100

    # example passing only required values which don't have defaults set
    try:
        # list user external policies attached to a user
        api_response = api_instance.list_user_external_principals(user_id)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->list_user_external_principals: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # list user external policies attached to a user
        api_response = api_instance.list_user_external_principals(user_id, prefix=prefix, after=after, amount=amount)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ExperimentalApi->list_user_external_principals: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  |
 **prefix** | **str**| return items prefixed with this value | [optional]
 **after** | **str**| return items after this value | [optional]
 **amount** | **int**| how many items to return | [optional] if omitted the server will use the default value of 100

### Return type

[**ExternalPrincipalList**](ExternalPrincipalList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | external principals list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**420** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

