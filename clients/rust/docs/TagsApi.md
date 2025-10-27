# \TagsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_tag**](TagsApi.md#create_tag) | **POST** /repositories/{repository}/tags | create tag
[**delete_tag**](TagsApi.md#delete_tag) | **DELETE** /repositories/{repository}/tags/{tag} | delete tag
[**get_tag**](TagsApi.md#get_tag) | **GET** /repositories/{repository}/tags/{tag} | get tag
[**list_tags**](TagsApi.md#list_tags) | **GET** /repositories/{repository}/tags | list tags



## create_tag

> models::Ref create_tag(repository, tag_creation)
create tag

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**tag_creation** | [**TagCreation**](TagCreation.md) |  | [required] |

### Return type

[**models::Ref**](Ref.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_tag

> delete_tag(repository, tag, force)
delete tag

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**tag** | **String** |  | [required] |
**force** | Option<**bool**> |  |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_tag

> models::Ref get_tag(repository, tag)
get tag

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**tag** | **String** |  | [required] |

### Return type

[**models::Ref**](Ref.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_tags

> models::RefList list_tags(repository, prefix, after, amount)
list tags

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::RefList**](RefList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

