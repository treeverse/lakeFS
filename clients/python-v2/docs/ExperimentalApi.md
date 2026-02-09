# lakefs_sdk_v2.ExperimentalApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**abort_presign_multipart_upload**](ExperimentalApi.md#abort_presign_multipart_upload) | **DELETE** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Abort a presign multipart upload
[**commit_async**](ExperimentalApi.md#commit_async) | **POST** /repositories/{repository}/branches/{branch}/commits/async | create commit asynchronously
[**commit_async_status**](ExperimentalApi.md#commit_async_status) | **GET** /repositories/{repository}/branches/{branch}/commits/async/{id}/status | get status of async commit operation
[**complete_presign_multipart_upload**](ExperimentalApi.md#complete_presign_multipart_upload) | **PUT** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Complete a presign multipart upload request
[**create_presign_multipart_upload**](ExperimentalApi.md#create_presign_multipart_upload) | **POST** /repositories/{repository}/branches/{branch}/staging/pmpu | Initiate a multipart upload
[**create_pull_request_0**](ExperimentalApi.md#create_pull_request_0) | **POST** /repositories/{repository}/pulls | create pull request
[**create_user_external_principal_1**](ExperimentalApi.md#create_user_external_principal_1) | **POST** /auth/users/{userId}/external/principals | attach external principal to user
[**delete_user_external_principal_1**](ExperimentalApi.md#delete_user_external_principal_1) | **DELETE** /auth/users/{userId}/external/principals | delete external principal from user
[**external_principal_login_0**](ExperimentalApi.md#external_principal_login_0) | **POST** /auth/external/principal/login | perform a login using an external authenticator
[**get_external_principal_1**](ExperimentalApi.md#get_external_principal_1) | **GET** /auth/external/principals | describe external principal by id
[**get_license_0**](ExperimentalApi.md#get_license_0) | **GET** /license | 
[**get_pull_request_0**](ExperimentalApi.md#get_pull_request_0) | **GET** /repositories/{repository}/pulls/{pull_request} | get pull request
[**get_token_from_mailbox_0**](ExperimentalApi.md#get_token_from_mailbox_0) | **GET** /auth/get-token/mailboxes/{mailbox} | receive the token after user has authenticated on redirect URL.
[**get_token_redirect_0**](ExperimentalApi.md#get_token_redirect_0) | **GET** /auth/get-token/start | start acquiring a token by logging in on a browser
[**hard_reset_branch**](ExperimentalApi.md#hard_reset_branch) | **PUT** /repositories/{repository}/branches/{branch}/hard_reset | hard reset branch
[**list_pull_requests_0**](ExperimentalApi.md#list_pull_requests_0) | **GET** /repositories/{repository}/pulls | list pull requests
[**list_user_external_principals_1**](ExperimentalApi.md#list_user_external_principals_1) | **GET** /auth/users/{userId}/external/principals/ls | list user external policies attached to a user
[**merge_into_branch_async**](ExperimentalApi.md#merge_into_branch_async) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch}/async | merge references asynchronously
[**merge_into_branch_async_status**](ExperimentalApi.md#merge_into_branch_async_status) | **GET** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch}/async/{id}/status | get status of async merge operation
[**merge_pull_request_0**](ExperimentalApi.md#merge_pull_request_0) | **PUT** /repositories/{repository}/pulls/{pull_request}/merge | merge pull request
[**release_token_to_mailbox_0**](ExperimentalApi.md#release_token_to_mailbox_0) | **GET** /auth/get-token/release-token/{loginRequestToken} | release a token for the current (authenticated) user to the mailbox of this login request.
[**sts_login**](ExperimentalApi.md#sts_login) | **POST** /sts/login | perform a login with STS
[**update_object_user_metadata_0**](ExperimentalApi.md#update_object_user_metadata_0) | **PUT** /repositories/{repository}/branches/{branch}/objects/stat/user_metadata | rewrite (all) object metadata
[**update_pull_request_0**](ExperimentalApi.md#update_pull_request_0) | **PATCH** /repositories/{repository}/pulls/{pull_request} | update pull request
[**upload_part**](ExperimentalApi.md#upload_part) | **PUT** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId}/parts/{partNumber} | 
[**upload_part_copy**](ExperimentalApi.md#upload_part_copy) | **PUT** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId}/parts/{partNumber}/copy | 


# **abort_presign_multipart_upload**
> abort_presign_multipart_upload(repository, branch, upload_id, path, abort_presign_multipart_upload=abort_presign_multipart_upload)

Abort a presign multipart upload

Aborts a presign multipart upload.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.abort_presign_multipart_upload import AbortPresignMultipartUpload
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    upload_id = 'upload_id_example' # str | 
    path = 'path_example' # str | relative to the branch
    abort_presign_multipart_upload = lakefs_sdk_v2.AbortPresignMultipartUpload() # AbortPresignMultipartUpload |  (optional)

    try:
        # Abort a presign multipart upload
        api_instance.abort_presign_multipart_upload(repository, branch, upload_id, path, abort_presign_multipart_upload=abort_presign_multipart_upload)
    except Exception as e:
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

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

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
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **commit_async**
> TaskCreation commit_async(repository, branch, commit_creation, source_metarange=source_metarange)

create commit asynchronously

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.commit_creation import CommitCreation
from lakefs_sdk_v2.models.task_creation import TaskCreation
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    commit_creation = lakefs_sdk_v2.CommitCreation() # CommitCreation | 
    source_metarange = 'source_metarange_example' # str | The source metarange to commit. Branch must not have uncommitted changes. (optional)

    try:
        # create commit asynchronously
        api_response = api_instance.commit_async(repository, branch, commit_creation, source_metarange=source_metarange)
        print("The response of ExperimentalApi->commit_async:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->commit_async: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **branch** | **str**|  | 
 **commit_creation** | [**CommitCreation**](CommitCreation.md)|  | 
 **source_metarange** | **str**| The source metarange to commit. Branch must not have uncommitted changes. | [optional] 

### Return type

[**TaskCreation**](TaskCreation.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**202** | commit task started |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **commit_async_status**
> CommitAsyncStatus commit_async_status(repository, branch, id)

get status of async commit operation

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.commit_async_status import CommitAsyncStatus
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    id = 'id_example' # str | Unique identifier of the commit async task

    try:
        # get status of async commit operation
        api_response = api_instance.commit_async_status(repository, branch, id)
        print("The response of ExperimentalApi->commit_async_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->commit_async_status: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **branch** | **str**|  | 
 **id** | **str**| Unique identifier of the commit async task | 

### Return type

[**CommitAsyncStatus**](CommitAsyncStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | commit task status |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**409** | Resource Conflicts With Target |  -  |
**412** | Precondition Failed |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **complete_presign_multipart_upload**
> ObjectStats complete_presign_multipart_upload(repository, branch, upload_id, path, complete_presign_multipart_upload=complete_presign_multipart_upload)

Complete a presign multipart upload request

Completes a presign multipart upload by assembling the uploaded parts.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.complete_presign_multipart_upload import CompletePresignMultipartUpload
from lakefs_sdk_v2.models.object_stats import ObjectStats
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    upload_id = 'upload_id_example' # str | 
    path = 'path_example' # str | relative to the branch
    complete_presign_multipart_upload = lakefs_sdk_v2.CompletePresignMultipartUpload() # CompletePresignMultipartUpload |  (optional)

    try:
        # Complete a presign multipart upload request
        api_response = api_instance.complete_presign_multipart_upload(repository, branch, upload_id, path, complete_presign_multipart_upload=complete_presign_multipart_upload)
        print("The response of ExperimentalApi->complete_presign_multipart_upload:\n")
        pprint(api_response)
    except Exception as e:
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

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

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
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_presign_multipart_upload**
> PresignMultipartUpload create_presign_multipart_upload(repository, branch, path, parts=parts)

Initiate a multipart upload

Initiates a multipart upload and returns an upload ID with presigned URLs for each part (optional). Part numbers starts with 1. Each part except the last one has minimum size depends on the underlying blockstore implementation. For example working with S3 blockstore, minimum size is 5MB (excluding the last part). 

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.presign_multipart_upload import PresignMultipartUpload
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    path = 'path_example' # str | relative to the branch
    parts = 56 # int | number of presigned URL parts required to upload (optional)

    try:
        # Initiate a multipart upload
        api_response = api_instance.create_presign_multipart_upload(repository, branch, path, parts=parts)
        print("The response of ExperimentalApi->create_presign_multipart_upload:\n")
        pprint(api_response)
    except Exception as e:
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

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

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
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_pull_request_0**
> PullRequestCreationResponse create_pull_request_0(repository, pull_request_creation)

create pull request

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.pull_request_creation import PullRequestCreation
from lakefs_sdk_v2.models.pull_request_creation_response import PullRequestCreationResponse
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    pull_request_creation = lakefs_sdk_v2.PullRequestCreation() # PullRequestCreation | 

    try:
        # create pull request
        api_response = api_instance.create_pull_request_0(repository, pull_request_creation)
        print("The response of ExperimentalApi->create_pull_request_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->create_pull_request_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **pull_request_creation** | [**PullRequestCreation**](PullRequestCreation.md)|  | 

### Return type

[**PullRequestCreationResponse**](PullRequestCreationResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | pull request created |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**409** | Resource Conflicts With Target |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_user_external_principal_1**
> create_user_external_principal_1(user_id, principal_id, external_principal_creation=external_principal_creation)

attach external principal to user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.external_principal_creation import ExternalPrincipalCreation
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    user_id = 'user_id_example' # str | 
    principal_id = 'principal_id_example' # str | 
    external_principal_creation = lakefs_sdk_v2.ExternalPrincipalCreation() # ExternalPrincipalCreation |  (optional)

    try:
        # attach external principal to user
        api_instance.create_user_external_principal_1(user_id, principal_id, external_principal_creation=external_principal_creation)
    except Exception as e:
        print("Exception when calling ExperimentalApi->create_user_external_principal_1: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  | 
 **principal_id** | **str**|  | 
 **external_principal_creation** | [**ExternalPrincipalCreation**](ExternalPrincipalCreation.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | external principal attached successfully |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**409** | Resource Conflicts With Target |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_user_external_principal_1**
> delete_user_external_principal_1(user_id, principal_id)

delete external principal from user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    user_id = 'user_id_example' # str | 
    principal_id = 'principal_id_example' # str | 

    try:
        # delete external principal from user
        api_instance.delete_user_external_principal_1(user_id, principal_id)
    except Exception as e:
        print("Exception when calling ExperimentalApi->delete_user_external_principal_1: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  | 
 **principal_id** | **str**|  | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | external principal detached successfully |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **external_principal_login_0**
> AuthenticationToken external_principal_login_0(external_login_information=external_login_information)

perform a login using an external authenticator

### Example


```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.authentication_token import AuthenticationToken
from lakefs_sdk_v2.models.external_login_information import ExternalLoginInformation
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    external_login_information = lakefs_sdk_v2.ExternalLoginInformation() # ExternalLoginInformation |  (optional)

    try:
        # perform a login using an external authenticator
        api_response = api_instance.external_principal_login_0(external_login_information=external_login_information)
        print("The response of ExperimentalApi->external_principal_login_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->external_principal_login_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **external_login_information** | [**ExternalLoginInformation**](ExternalLoginInformation.md)|  | [optional] 

### Return type

[**AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful external login |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_external_principal_1**
> ExternalPrincipal get_external_principal_1(principal_id)

describe external principal by id

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.external_principal import ExternalPrincipal
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    principal_id = 'principal_id_example' # str | 

    try:
        # describe external principal by id
        api_response = api_instance.get_external_principal_1(principal_id)
        print("The response of ExperimentalApi->get_external_principal_1:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->get_external_principal_1: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_id** | **str**|  | 

### Return type

[**ExternalPrincipal**](ExternalPrincipal.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | external principal |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_license_0**
> License get_license_0()



retrieve lakeFS license information

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.license import License
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)

    try:
        api_response = api_instance.get_license_0()
        print("The response of ExperimentalApi->get_license_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->get_license_0: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**License**](License.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS configuration |  -  |
**401** | Unauthorized |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_pull_request_0**
> PullRequest get_pull_request_0(repository, pull_request)

get pull request

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.pull_request import PullRequest
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    pull_request = 'pull_request_example' # str | pull request id

    try:
        # get pull request
        api_response = api_instance.get_pull_request_0(repository, pull_request)
        print("The response of ExperimentalApi->get_pull_request_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->get_pull_request_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **pull_request** | **str**| pull request id | 

### Return type

[**PullRequest**](PullRequest.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | pull request |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_token_from_mailbox_0**
> AuthenticationToken get_token_from_mailbox_0(mailbox)

receive the token after user has authenticated on redirect URL.

### Example


```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.authentication_token import AuthenticationToken
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    mailbox = 'mailbox_example' # str | mailbox returned by getTokenRedirect

    try:
        # receive the token after user has authenticated on redirect URL.
        api_response = api_instance.get_token_from_mailbox_0(mailbox)
        print("The response of ExperimentalApi->get_token_from_mailbox_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->get_token_from_mailbox_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mailbox** | **str**| mailbox returned by getTokenRedirect | 

### Return type

[**AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | user successfully logged in |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_token_redirect_0**
> Error get_token_redirect_0()

start acquiring a token by logging in on a browser

### Example


```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.error import Error
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)

    try:
        # start acquiring a token by logging in on a browser
        api_response = api_instance.get_token_redirect_0()
        print("The response of ExperimentalApi->get_token_redirect_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->get_token_redirect_0: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**Error**](Error.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**303** | login on this page, await results on the mailbox URL |  * Location - redirect to S3 <br>  * X-LakeFS-Mailbox - GET the token from this mailbox.  Keep the mailbox SECRET! <br>  |
**401** | Unauthorized |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **hard_reset_branch**
> hard_reset_branch(repository, branch, ref, force=force)

hard reset branch

Relocate branch to refer to ref.  Branch must not contain uncommitted data.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    ref = 'ref_example' # str | After reset, branch will point at this reference.
    force = False # bool |  (optional) (default to False)

    try:
        # hard reset branch
        api_instance.hard_reset_branch(repository, branch, ref, force=force)
    except Exception as e:
        print("Exception when calling ExperimentalApi->hard_reset_branch: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **branch** | **str**|  | 
 **ref** | **str**| After reset, branch will point at this reference. | 
 **force** | **bool**|  | [optional] [default to False]

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

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
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_pull_requests_0**
> PullRequestsList list_pull_requests_0(repository, prefix=prefix, after=after, amount=amount, status=status)

list pull requests

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.pull_requests_list import PullRequestsList
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    prefix = 'prefix_example' # str | return items prefixed with this value (optional)
    after = 'after_example' # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) (default to 100)
    status = all # str |  (optional) (default to all)

    try:
        # list pull requests
        api_response = api_instance.list_pull_requests_0(repository, prefix=prefix, after=after, amount=amount, status=status)
        print("The response of ExperimentalApi->list_pull_requests_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->list_pull_requests_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **prefix** | **str**| return items prefixed with this value | [optional] 
 **after** | **str**| return items after this value | [optional] 
 **amount** | **int**| how many items to return | [optional] [default to 100]
 **status** | **str**|  | [optional] [default to all]

### Return type

[**PullRequestsList**](PullRequestsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | list of pull requests |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_user_external_principals_1**
> ExternalPrincipalList list_user_external_principals_1(user_id, prefix=prefix, after=after, amount=amount)

list user external policies attached to a user

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.external_principal_list import ExternalPrincipalList
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    user_id = 'user_id_example' # str | 
    prefix = 'prefix_example' # str | return items prefixed with this value (optional)
    after = 'after_example' # str | return items after this value (optional)
    amount = 100 # int | how many items to return (optional) (default to 100)

    try:
        # list user external policies attached to a user
        api_response = api_instance.list_user_external_principals_1(user_id, prefix=prefix, after=after, amount=amount)
        print("The response of ExperimentalApi->list_user_external_principals_1:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->list_user_external_principals_1: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_id** | **str**|  | 
 **prefix** | **str**| return items prefixed with this value | [optional] 
 **after** | **str**| return items after this value | [optional] 
 **amount** | **int**| how many items to return | [optional] [default to 100]

### Return type

[**ExternalPrincipalList**](ExternalPrincipalList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | external principals list |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **merge_into_branch_async**
> TaskCreation merge_into_branch_async(repository, source_ref, destination_branch, merge=merge)

merge references asynchronously

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.merge import Merge
from lakefs_sdk_v2.models.task_creation import TaskCreation
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    source_ref = 'source_ref_example' # str | source ref
    destination_branch = 'destination_branch_example' # str | destination branch name
    merge = lakefs_sdk_v2.Merge() # Merge |  (optional)

    try:
        # merge references asynchronously
        api_response = api_instance.merge_into_branch_async(repository, source_ref, destination_branch, merge=merge)
        print("The response of ExperimentalApi->merge_into_branch_async:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->merge_into_branch_async: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **source_ref** | **str**| source ref | 
 **destination_branch** | **str**| destination branch name | 
 **merge** | [**Merge**](Merge.md)|  | [optional] 

### Return type

[**TaskCreation**](TaskCreation.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**202** | merge task started |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **merge_into_branch_async_status**
> MergeAsyncStatus merge_into_branch_async_status(repository, source_ref, destination_branch, id)

get status of async merge operation

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.merge_async_status import MergeAsyncStatus
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    source_ref = 'source_ref_example' # str | source ref
    destination_branch = 'destination_branch_example' # str | destination branch name
    id = 'id_example' # str | Unique identifier of the merge async task

    try:
        # get status of async merge operation
        api_response = api_instance.merge_into_branch_async_status(repository, source_ref, destination_branch, id)
        print("The response of ExperimentalApi->merge_into_branch_async_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->merge_into_branch_async_status: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **source_ref** | **str**| source ref | 
 **destination_branch** | **str**| destination branch name | 
 **id** | **str**| Unique identifier of the merge async task | 

### Return type

[**MergeAsyncStatus**](MergeAsyncStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | merge task status |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**409** | Resource Conflicts With Target |  -  |
**412** | Precondition Failed |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **merge_pull_request_0**
> MergeResult merge_pull_request_0(repository, pull_request)

merge pull request

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.merge_result import MergeResult
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    pull_request = 'pull_request_example' # str | pull request id

    try:
        # merge pull request
        api_response = api_instance.merge_pull_request_0(repository, pull_request)
        print("The response of ExperimentalApi->merge_pull_request_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->merge_pull_request_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **pull_request** | **str**| pull request id | 

### Return type

[**MergeResult**](MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
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
**412** | precondition failed |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **release_token_to_mailbox_0**
> release_token_to_mailbox_0(login_request_token)

release a token for the current (authenticated) user to the mailbox of this login request.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    login_request_token = 'login_request_token_example' # str | login request token returned by getTokenRedirect.

    try:
        # release a token for the current (authenticated) user to the mailbox of this login request.
        api_instance.release_token_to_mailbox_0(login_request_token)
    except Exception as e:
        print("Exception when calling ExperimentalApi->release_token_to_mailbox_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **login_request_token** | **str**| login request token returned by getTokenRedirect. | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | token released |  -  |
**401** | Unauthorized |  -  |
**429** | too many requests |  -  |
**501** | Not Implemented |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **sts_login**
> AuthenticationToken sts_login(sts_auth_request)

perform a login with STS

### Example


```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.authentication_token import AuthenticationToken
from lakefs_sdk_v2.models.sts_auth_request import StsAuthRequest
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    sts_auth_request = lakefs_sdk_v2.StsAuthRequest() # StsAuthRequest | 

    try:
        # perform a login with STS
        api_response = api_instance.sts_login(sts_auth_request)
        print("The response of ExperimentalApi->sts_login:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->sts_login: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sts_auth_request** | [**StsAuthRequest**](StsAuthRequest.md)|  | 

### Return type

[**AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful STS login |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_object_user_metadata_0**
> update_object_user_metadata_0(repository, branch, path, update_object_user_metadata)

rewrite (all) object metadata

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.update_object_user_metadata import UpdateObjectUserMetadata
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | branch to update
    path = 'path_example' # str | path to object relative to the branch
    update_object_user_metadata = lakefs_sdk_v2.UpdateObjectUserMetadata() # UpdateObjectUserMetadata | 

    try:
        # rewrite (all) object metadata
        api_instance.update_object_user_metadata_0(repository, branch, path, update_object_user_metadata)
    except Exception as e:
        print("Exception when calling ExperimentalApi->update_object_user_metadata_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **branch** | **str**| branch to update | 
 **path** | **str**| path to object relative to the branch | 
 **update_object_user_metadata** | [**UpdateObjectUserMetadata**](UpdateObjectUserMetadata.md)|  | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | User metadata updated |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**400** | Bad Request |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_pull_request_0**
> update_pull_request_0(repository, pull_request, pull_request_basic)

update pull request

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.pull_request_basic import PullRequestBasic
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    pull_request = 'pull_request_example' # str | pull request id
    pull_request_basic = lakefs_sdk_v2.PullRequestBasic() # PullRequestBasic | 

    try:
        # update pull request
        api_instance.update_pull_request_0(repository, pull_request, pull_request_basic)
    except Exception as e:
        print("Exception when calling ExperimentalApi->update_pull_request_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **pull_request** | **str**| pull request id | 
 **pull_request_basic** | [**PullRequestBasic**](PullRequestBasic.md)|  | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | pull request updated successfully |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**403** | Forbidden |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_part**
> UploadTo upload_part(repository, branch, upload_id, path, part_number, upload_part_from)



Return a presigned URL to upload into a presigned multipart upload.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.upload_part_from import UploadPartFrom
from lakefs_sdk_v2.models.upload_to import UploadTo
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    upload_id = 'upload_id_example' # str | 
    path = 'path_example' # str | 
    part_number = 56 # int | 
    upload_part_from = lakefs_sdk_v2.UploadPartFrom() # UploadPartFrom | 

    try:
        api_response = api_instance.upload_part(repository, branch, upload_id, path, part_number, upload_part_from)
        print("The response of ExperimentalApi->upload_part:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ExperimentalApi->upload_part: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **branch** | **str**|  | 
 **upload_id** | **str**|  | 
 **path** | **str**|  | 
 **part_number** | **int**|  | 
 **upload_part_from** | [**UploadPartFrom**](UploadPartFrom.md)|  | 

### Return type

[**UploadTo**](UploadTo.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | presigned URL to use for upload |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_part_copy**
> upload_part_copy(repository, branch, upload_id, path, part_number, upload_part_copy_from)



Upload a part by copying part of another object.

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import lakefs_sdk_v2
from lakefs_sdk_v2.models.upload_part_copy_from import UploadPartCopyFrom
from lakefs_sdk_v2.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk_v2.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk_v2.Configuration(
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
configuration = lakefs_sdk_v2.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk_v2.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk_v2.ExperimentalApi(api_client)
    repository = 'repository_example' # str | 
    branch = 'branch_example' # str | 
    upload_id = 'upload_id_example' # str | 
    path = 'path_example' # str | 
    part_number = 56 # int | 
    upload_part_copy_from = lakefs_sdk_v2.UploadPartCopyFrom() # UploadPartCopyFrom | 

    try:
        api_instance.upload_part_copy(repository, branch, upload_id, path, part_number, upload_part_copy_from)
    except Exception as e:
        print("Exception when calling ExperimentalApi->upload_part_copy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  | 
 **branch** | **str**|  | 
 **upload_id** | **str**|  | 
 **path** | **str**|  | 
 **part_number** | **int**|  | 
 **upload_part_copy_from** | [**UploadPartCopyFrom**](UploadPartCopyFrom.md)|  | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | part copied |  * ETag -  <br>  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**429** | too many requests |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

