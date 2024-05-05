# \ExternalApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_user_external_principal**](ExternalApi.md#create_user_external_principal) | **POST** /auth/users/{userId}/external/principals | attach external principal to user
[**delete_user_external_principal**](ExternalApi.md#delete_user_external_principal) | **DELETE** /auth/users/{userId}/external/principals | delete external principal from user
[**external_principal_login**](ExternalApi.md#external_principal_login) | **POST** /auth/external/principal/login | perform a login using an external authenticator
[**get_external_principal**](ExternalApi.md#get_external_principal) | **GET** /auth/external/principals | describe external principal by id
[**list_user_external_principals**](ExternalApi.md#list_user_external_principals) | **GET** /auth/users/{userId}/external/principals/ls | list user external policies attached to a user



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

