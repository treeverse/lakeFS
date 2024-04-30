# \RefsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**diff_refs**](RefsApi.md#diff_refs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
[**find_merge_base**](RefsApi.md#find_merge_base) | **GET** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | find the merge base for 2 references
[**log_commits**](RefsApi.md#log_commits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref. If both objects and prefixes are empty, return all commits.
[**merge_into_branch**](RefsApi.md#merge_into_branch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references



## diff_refs

> models::DiffList diff_refs(repository, left_ref, right_ref, after, amount, prefix, delimiter, r#type)
diff references

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**left_ref** | **String** | a reference (could be either a branch or a commit ID) | [required] |
**right_ref** | **String** | a reference (could be either a branch or a commit ID) to compare against | [required] |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**prefix** | Option<**String**> | return items prefixed with this value |  |
**delimiter** | Option<**String**> | delimiter used to group common prefixes by |  |
**r#type** | Option<**String**> |  |  |[default to three_dot]

### Return type

[**models::DiffList**](DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## find_merge_base

> models::FindMergeBaseResult find_merge_base(repository, source_ref, destination_branch)
find the merge base for 2 references

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**source_ref** | **String** | source ref | [required] |
**destination_branch** | **String** | destination branch name | [required] |

### Return type

[**models::FindMergeBaseResult**](FindMergeBaseResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## log_commits

> models::CommitList log_commits(repository, r#ref, after, amount, objects, prefixes, limit, first_parent, since, stop_at)
get commit log from ref. If both objects and prefixes are empty, return all commits.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**r#ref** | **String** |  | [required] |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**objects** | Option<[**Vec<String>**](String.md)> | list of paths, each element is a path of a specific object |  |
**prefixes** | Option<[**Vec<String>**](String.md)> | list of paths, each element is a path of a prefix |  |
**limit** | Option<**bool**> | limit the number of items in return to 'amount'. Without further indication on actual number of items. |  |
**first_parent** | Option<**bool**> | if set to true, follow only the first parent upon reaching a merge commit |  |
**since** | Option<**String**> | Show commits more recent than a specific date-time. In case used with stop_at parameter, will stop at the first commit that meets any of the conditions. |  |
**stop_at** | Option<**String**> | A reference to stop at. In case used with since parameter, will stop at the first commit that meets any of the conditions. |  |

### Return type

[**models::CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## merge_into_branch

> models::MergeResult merge_into_branch(repository, source_ref, destination_branch, merge)
merge references

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**source_ref** | **String** | source ref | [required] |
**destination_branch** | **String** | destination branch name | [required] |
**merge** | Option<[**Merge**](Merge.md)> |  |  |

### Return type

[**models::MergeResult**](MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

