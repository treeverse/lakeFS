# \ExperimentalApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**abort_presign_multipart_upload**](ExperimentalApi.md#abort_presign_multipart_upload) | **DELETE** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Abort a presign multipart upload
[**complete_presign_multipart_upload**](ExperimentalApi.md#complete_presign_multipart_upload) | **PUT** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Complete a presign multipart upload request
[**create_presign_multipart_upload**](ExperimentalApi.md#create_presign_multipart_upload) | **POST** /repositories/{repository}/branches/{branch}/staging/pmpu | Initiate a multipart upload
[**create_pull_request**](ExperimentalApi.md#create_pull_request) | **POST** /repositories/{repository}/pulls | create pull request
[**create_user_external_principal**](ExperimentalApi.md#create_user_external_principal) | **POST** /auth/users/{userId}/external/principals | attach external principal to user
[**delete_user_external_principal**](ExperimentalApi.md#delete_user_external_principal) | **DELETE** /auth/users/{userId}/external/principals | delete external principal from user
[**external_principal_login**](ExperimentalApi.md#external_principal_login) | **POST** /auth/external/principal/login | perform a login using an external authenticator
[**get_external_principal**](ExperimentalApi.md#get_external_principal) | **GET** /auth/external/principals | describe external principal by id
[**get_pull_request**](ExperimentalApi.md#get_pull_request) | **GET** /repositories/{repository}/pulls/{pull_request} | get pull request
[**hard_reset_branch**](ExperimentalApi.md#hard_reset_branch) | **PUT** /repositories/{repository}/branches/{branch}/hard_reset | hard reset branch
[**list_pull_requests**](ExperimentalApi.md#list_pull_requests) | **GET** /repositories/{repository}/pulls | list pull requests
[**list_user_external_principals**](ExperimentalApi.md#list_user_external_principals) | **GET** /auth/users/{userId}/external/principals/ls | list user external policies attached to a user
[**sts_login**](ExperimentalApi.md#sts_login) | **POST** /sts/login | perform a login with STS
[**update_pull_request**](ExperimentalApi.md#update_pull_request) | **PATCH** /repositories/{repository}/pulls/{pull_request} | update pull request



## abort_presign_multipart_upload

> abort_presign_multipart_upload(repository, branch, upload_id, path, abort_presign_multipart_upload)
Abort a presign multipart upload

Aborts a presign multipart upload.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**upload_id** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |
**abort_presign_multipart_upload** | Option<[**AbortPresignMultipartUpload**](AbortPresignMultipartUpload.md)> |  |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## complete_presign_multipart_upload

> models::ObjectStats complete_presign_multipart_upload(repository, branch, upload_id, path, complete_presign_multipart_upload)
Complete a presign multipart upload request

Completes a presign multipart upload by assembling the uploaded parts.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**upload_id** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |
**complete_presign_multipart_upload** | Option<[**CompletePresignMultipartUpload**](CompletePresignMultipartUpload.md)> |  |  |

### Return type

[**models::ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_presign_multipart_upload

> models::PresignMultipartUpload create_presign_multipart_upload(repository, branch, path, parts)
Initiate a multipart upload

Initiates a multipart upload and returns an upload ID with presigned URLs for each part (optional). Part numbers starts with 1. Each part except the last one has minimum size depends on the underlying blockstore implementation. For example working with S3 blockstore, minimum size is 5MB (excluding the last part). 

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |
**parts** | Option<**i32**> | number of presigned URL parts required to upload |  |

### Return type

[**models::PresignMultipartUpload**](PresignMultipartUpload.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_pull_request

> String create_pull_request(repository, pull_request_creation)
create pull request

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**pull_request_creation** | [**PullRequestCreation**](PullRequestCreation.md) |  | [required] |

### Return type

**String**

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: text/html, application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_user_external_principal

> create_user_external_principal(user_id, principal_id, external_principal_creation)
attach external principal to user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**principal_id** | **String** |  | [required] |
**external_principal_creation** | Option<[**ExternalPrincipalCreation**](ExternalPrincipalCreation.md)> |  |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_user_external_principal

> delete_user_external_principal(user_id, principal_id)
delete external principal from user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**principal_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## external_principal_login

> models::AuthenticationToken external_principal_login(external_login_information)
perform a login using an external authenticator

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**external_login_information** | Option<[**ExternalLoginInformation**](ExternalLoginInformation.md)> |  |  |

### Return type

[**models::AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_external_principal

> models::ExternalPrincipal get_external_principal(principal_id)
describe external principal by id

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**principal_id** | **String** |  | [required] |

### Return type

[**models::ExternalPrincipal**](ExternalPrincipal.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_pull_request

> models::PullRequest get_pull_request(repository, pull_request)
get pull request

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**pull_request** | **String** | pull request id | [required] |

### Return type

[**models::PullRequest**](PullRequest.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## hard_reset_branch

> hard_reset_branch(repository, branch, r#ref, force)
hard reset branch

Relocate branch to refer to ref.  Branch must not contain uncommitted data.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**r#ref** | **String** | After reset, branch will point at this reference. | [required] |
**force** | Option<**bool**> |  |  |[default to false]

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_pull_requests

> models::PullRequestsList list_pull_requests(repository, prefix, after, amount, status)
list pull requests

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**status** | Option<**String**> |  |  |[default to all]

### Return type

[**models::PullRequestsList**](PullRequestsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_user_external_principals

> models::ExternalPrincipalList list_user_external_principals(user_id, prefix, after, amount)
list user external policies attached to a user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::ExternalPrincipalList**](ExternalPrincipalList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## sts_login

> models::AuthenticationToken sts_login(sts_auth_request)
perform a login with STS

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**sts_auth_request** | [**StsAuthRequest**](StsAuthRequest.md) |  | [required] |

### Return type

[**models::AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_pull_request

> update_pull_request(repository, pull_request, pull_request_basic)
update pull request

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**pull_request** | **String** | pull request id | [required] |
**pull_request_basic** | [**PullRequestBasic**](PullRequestBasic.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

