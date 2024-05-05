# \AuthApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_group_membership**](AuthApi.md#add_group_membership) | **PUT** /auth/groups/{groupId}/members/{userId} | add group membership
[**attach_policy_to_group**](AuthApi.md#attach_policy_to_group) | **PUT** /auth/groups/{groupId}/policies/{policyId} | attach policy to group
[**attach_policy_to_user**](AuthApi.md#attach_policy_to_user) | **PUT** /auth/users/{userId}/policies/{policyId} | attach policy to user
[**create_credentials**](AuthApi.md#create_credentials) | **POST** /auth/users/{userId}/credentials | create credentials
[**create_group**](AuthApi.md#create_group) | **POST** /auth/groups | create group
[**create_policy**](AuthApi.md#create_policy) | **POST** /auth/policies | create policy
[**create_user**](AuthApi.md#create_user) | **POST** /auth/users | create user
[**create_user_external_principal**](AuthApi.md#create_user_external_principal) | **POST** /auth/users/{userId}/external/principals | attach external principal to user
[**delete_credentials**](AuthApi.md#delete_credentials) | **DELETE** /auth/users/{userId}/credentials/{accessKeyId} | delete credentials
[**delete_group**](AuthApi.md#delete_group) | **DELETE** /auth/groups/{groupId} | delete group
[**delete_group_membership**](AuthApi.md#delete_group_membership) | **DELETE** /auth/groups/{groupId}/members/{userId} | delete group membership
[**delete_policy**](AuthApi.md#delete_policy) | **DELETE** /auth/policies/{policyId} | delete policy
[**delete_user**](AuthApi.md#delete_user) | **DELETE** /auth/users/{userId} | delete user
[**delete_user_external_principal**](AuthApi.md#delete_user_external_principal) | **DELETE** /auth/users/{userId}/external/principals | delete external principal from user
[**detach_policy_from_group**](AuthApi.md#detach_policy_from_group) | **DELETE** /auth/groups/{groupId}/policies/{policyId} | detach policy from group
[**detach_policy_from_user**](AuthApi.md#detach_policy_from_user) | **DELETE** /auth/users/{userId}/policies/{policyId} | detach policy from user
[**external_principal_login**](AuthApi.md#external_principal_login) | **POST** /auth/external/principal/login | perform a login using an external authenticator
[**get_credentials**](AuthApi.md#get_credentials) | **GET** /auth/users/{userId}/credentials/{accessKeyId} | get credentials
[**get_current_user**](AuthApi.md#get_current_user) | **GET** /user | get current user
[**get_external_principal**](AuthApi.md#get_external_principal) | **GET** /auth/external/principals | describe external principal by id
[**get_group**](AuthApi.md#get_group) | **GET** /auth/groups/{groupId} | get group
[**get_group_acl**](AuthApi.md#get_group_acl) | **GET** /auth/groups/{groupId}/acl | get ACL of group
[**get_policy**](AuthApi.md#get_policy) | **GET** /auth/policies/{policyId} | get policy
[**get_user**](AuthApi.md#get_user) | **GET** /auth/users/{userId} | get user
[**list_group_members**](AuthApi.md#list_group_members) | **GET** /auth/groups/{groupId}/members | list group members
[**list_group_policies**](AuthApi.md#list_group_policies) | **GET** /auth/groups/{groupId}/policies | list group policies
[**list_groups**](AuthApi.md#list_groups) | **GET** /auth/groups | list groups
[**list_policies**](AuthApi.md#list_policies) | **GET** /auth/policies | list policies
[**list_user_credentials**](AuthApi.md#list_user_credentials) | **GET** /auth/users/{userId}/credentials | list user credentials
[**list_user_external_principals**](AuthApi.md#list_user_external_principals) | **GET** /auth/users/{userId}/external/principals/ls | list user external policies attached to a user
[**list_user_groups**](AuthApi.md#list_user_groups) | **GET** /auth/users/{userId}/groups | list user groups
[**list_user_policies**](AuthApi.md#list_user_policies) | **GET** /auth/users/{userId}/policies | list user policies
[**list_users**](AuthApi.md#list_users) | **GET** /auth/users | list users
[**login**](AuthApi.md#login) | **POST** /auth/login | perform a login
[**set_group_acl**](AuthApi.md#set_group_acl) | **POST** /auth/groups/{groupId}/acl | set ACL of group
[**update_policy**](AuthApi.md#update_policy) | **PUT** /auth/policies/{policyId} | update policy



## add_group_membership

> add_group_membership(group_id, user_id)
add group membership

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**user_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## attach_policy_to_group

> attach_policy_to_group(group_id, policy_id)
attach policy to group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**policy_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## attach_policy_to_user

> attach_policy_to_user(user_id, policy_id)
attach policy to user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**policy_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_credentials

> models::CredentialsWithSecret create_credentials(user_id)
create credentials

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |

### Return type

[**models::CredentialsWithSecret**](CredentialsWithSecret.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_group

> models::Group create_group(group_creation)
create group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_creation** | Option<[**GroupCreation**](GroupCreation.md)> |  |  |

### Return type

[**models::Group**](Group.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_policy

> models::Policy create_policy(policy)
create policy

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**policy** | [**Policy**](Policy.md) |  | [required] |

### Return type

[**models::Policy**](Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_user

> models::User create_user(user_creation)
create user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_creation** | Option<[**UserCreation**](UserCreation.md)> |  |  |

### Return type

[**models::User**](User.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

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


## delete_credentials

> delete_credentials(user_id, access_key_id)
delete credentials

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**access_key_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_group

> delete_group(group_id)
delete group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_group_membership

> delete_group_membership(group_id, user_id)
delete group membership

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**user_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_policy

> delete_policy(policy_id)
delete policy

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**policy_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_user

> delete_user(user_id)
delete user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
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


## detach_policy_from_group

> detach_policy_from_group(group_id, policy_id)
detach policy from group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**policy_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## detach_policy_from_user

> detach_policy_from_user(user_id, policy_id)
detach policy from user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**policy_id** | **String** |  | [required] |

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


## get_credentials

> models::Credentials get_credentials(user_id, access_key_id)
get credentials

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**access_key_id** | **String** |  | [required] |

### Return type

[**models::Credentials**](Credentials.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_current_user

> models::CurrentUser get_current_user()
get current user

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::CurrentUser**](CurrentUser.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
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


## get_group

> models::Group get_group(group_id)
get group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |

### Return type

[**models::Group**](Group.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_group_acl

> models::Acl get_group_acl(group_id)
get ACL of group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |

### Return type

[**models::Acl**](ACL.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_policy

> models::Policy get_policy(policy_id)
get policy

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**policy_id** | **String** |  | [required] |

### Return type

[**models::Policy**](Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_user

> models::User get_user(user_id)
get user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |

### Return type

[**models::User**](User.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_group_members

> models::UserList list_group_members(group_id, prefix, after, amount)
list group members

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::UserList**](UserList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_group_policies

> models::PolicyList list_group_policies(group_id, prefix, after, amount)
list group policies

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::PolicyList**](PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_groups

> models::GroupList list_groups(prefix, after, amount)
list groups

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::GroupList**](GroupList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_policies

> models::PolicyList list_policies(prefix, after, amount)
list policies

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::PolicyList**](PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_user_credentials

> models::CredentialsList list_user_credentials(user_id, prefix, after, amount)
list user credentials

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::CredentialsList**](CredentialsList.md)

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


## list_user_groups

> models::GroupList list_user_groups(user_id, prefix, after, amount)
list user groups

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::GroupList**](GroupList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_user_policies

> models::PolicyList list_user_policies(user_id, prefix, after, amount, effective)
list user policies

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_id** | **String** |  | [required] |
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**effective** | Option<**bool**> | will return all distinct policies attached to the user or any of its groups |  |[default to false]

### Return type

[**models::PolicyList**](PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_users

> models::UserList list_users(prefix, after, amount)
list users

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]

### Return type

[**models::UserList**](UserList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## login

> models::AuthenticationToken login(login_information)
perform a login

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**login_information** | Option<[**LoginInformation**](LoginInformation.md)> |  |  |

### Return type

[**models::AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_group_acl

> set_group_acl(group_id, acl)
set ACL of group

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**group_id** | **String** |  | [required] |
**acl** | [**Acl**](Acl.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_policy

> models::Policy update_policy(policy_id, policy)
update policy

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**policy_id** | **String** |  | [required] |
**policy** | [**Policy**](Policy.md) |  | [required] |

### Return type

[**models::Policy**](Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

