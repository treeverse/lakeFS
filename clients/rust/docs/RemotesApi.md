# \RemotesApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**pull_iceberg_table**](RemotesApi.md#pull_iceberg_table) | **POST** /iceberg/remotes/{catalog}/pull | take a table previously pushed from lakeFS into a remote catalog, and pull its state back into the originating lakeFS repository
[**push_iceberg_table**](RemotesApi.md#push_iceberg_table) | **POST** /iceberg/remotes/{catalog}/push | register existing lakeFS table in remote catalog



## pull_iceberg_table

> pull_iceberg_table(catalog, iceberg_pull_request)
take a table previously pushed from lakeFS into a remote catalog, and pull its state back into the originating lakeFS repository

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**catalog** | **String** |  | [required] |
**iceberg_pull_request** | Option<[**IcebergPullRequest**](IcebergPullRequest.md)> |  |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## push_iceberg_table

> push_iceberg_table(catalog, iceberg_push_request)
register existing lakeFS table in remote catalog

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**catalog** | **String** |  | [required] |
**iceberg_push_request** | Option<[**IcebergPushRequest**](IcebergPushRequest.md)> |  |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

