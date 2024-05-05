# \ActionsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_run**](ActionsApi.md#get_run) | **GET** /repositories/{repository}/actions/runs/{run_id} | get a run
[**get_run_hook_output**](ActionsApi.md#get_run_hook_output) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks/{hook_run_id}/output | get run hook output
[**list_repository_runs**](ActionsApi.md#list_repository_runs) | **GET** /repositories/{repository}/actions/runs | list runs
[**list_run_hooks**](ActionsApi.md#list_run_hooks) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks | list run hooks



## get_run

> models::ActionRun get_run(repository, run_id)
get a run

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**run_id** | **String** |  | [required] |

### Return type

[**models::ActionRun**](ActionRun.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_run_hook_output

> std::path::PathBuf get_run_hook_output(repository, run_id, hook_run_id)
get run hook output

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**run_id** | **String** |  | [required] |
**hook_run_id** | **String** |  | [required] |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream, application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_repository_runs

> models::ActionRunList list_repository_runs(repository, after, amount, branch, commit)
list runs

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**branch** | Option<**String**> |  |  |
**commit** | Option<**String**> |  |  |

### Return type

[**models::ActionRunList**](ActionRunList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_run_hooks

> models::HookRunList list_run_hooks(repository, run_id, after, amount)
list run hooks

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**run_id** | **String** |  | [required] |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::HookRunList**](HookRunList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

