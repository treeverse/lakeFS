# \CommitsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**commit**](CommitsApi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit
[**commit_async**](CommitsApi.md#commit_async) | **POST** /repositories/{repository}/branches/{branch}/commits/async | create commit asynchronously
[**commit_status**](CommitsApi.md#commit_status) | **GET** /repositories/{repository}/branches/{branch}/commits/status | get status of async commit operation
[**get_commit**](CommitsApi.md#get_commit) | **GET** /repositories/{repository}/commits/{commitId} | get commit



## commit

> models::Commit commit(repository, branch, commit_creation, source_metarange)
create commit

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**commit_creation** | [**CommitCreation**](CommitCreation.md) |  | [required] |
**source_metarange** | Option<**String**> | The source metarange to commit. Branch must not have uncommitted changes. |  |

### Return type

[**models::Commit**](Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## commit_async

> models::TaskCreation commit_async(repository, branch, commit_creation, source_metarange)
create commit asynchronously

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**commit_creation** | [**CommitCreation**](CommitCreation.md) |  | [required] |
**source_metarange** | Option<**String**> | The source metarange to commit. Branch must not have uncommitted changes. |  |

### Return type

[**models::TaskCreation**](TaskCreation.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## commit_status

> models::CommitAsyncStatus commit_status(repository, branch, id)
get status of async commit operation

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**id** | **String** | Unique identifier of the commit task | [required] |

### Return type

[**models::CommitAsyncStatus**](CommitAsyncStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_commit

> models::Commit get_commit(repository, commit_id)
get commit

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**commit_id** | **String** |  | [required] |

### Return type

[**models::Commit**](Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

