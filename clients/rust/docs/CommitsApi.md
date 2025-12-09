# \CommitsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**commit**](CommitsApi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit
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

