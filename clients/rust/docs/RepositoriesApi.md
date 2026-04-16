# \RepositoriesApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_repository**](RepositoriesApi.md#create_repository) | **POST** /repositories | create repository
[**delete_gc_rules**](RepositoriesApi.md#delete_gc_rules) | **DELETE** /repositories/{repository}/settings/gc_rules | 
[**delete_repository**](RepositoriesApi.md#delete_repository) | **DELETE** /repositories/{repository} | delete repository
[**dump_status**](RepositoriesApi.md#dump_status) | **GET** /repositories/{repository}/dump | Status of a repository dump task
[**dump_submit**](RepositoriesApi.md#dump_submit) | **POST** /repositories/{repository}/dump | Backup the repository metadata (tags, commits, branches) and save the backup to the object store.
[**get_branch_protection_rules**](RepositoriesApi.md#get_branch_protection_rules) | **GET** /repositories/{repository}/settings/branch_protection | get branch protection rules
[**get_gc_rules**](RepositoriesApi.md#get_gc_rules) | **GET** /repositories/{repository}/settings/gc_rules | get repository GC rules
[**get_repository**](RepositoriesApi.md#get_repository) | **GET** /repositories/{repository} | get repository
[**get_repository_metadata**](RepositoriesApi.md#get_repository_metadata) | **GET** /repositories/{repository}/metadata | get repository metadata
[**list_repositories**](RepositoriesApi.md#list_repositories) | **GET** /repositories | list repositories
[**restore_status**](RepositoriesApi.md#restore_status) | **GET** /repositories/{repository}/restore | Status of a restore request
[**restore_submit**](RepositoriesApi.md#restore_submit) | **POST** /repositories/{repository}/restore | Restore repository from a dump in the object store
[**set_branch_protection_rules**](RepositoriesApi.md#set_branch_protection_rules) | **PUT** /repositories/{repository}/settings/branch_protection | 
[**set_gc_rules**](RepositoriesApi.md#set_gc_rules) | **PUT** /repositories/{repository}/settings/gc_rules | 



## create_repository

> models::Repository create_repository(repository_creation, bare)
create repository

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository_creation** | [**RepositoryCreation**](RepositoryCreation.md) |  | [required] |
**bare** | Option<**bool**> | If true, create a bare repository with no initial commit and branch |  |[default to false]

### Return type

[**models::Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_gc_rules

> delete_gc_rules(repository)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_repository

> delete_repository(repository, force)
delete repository

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**force** | Option<**bool**> | Bypass read-only protection and delete the repository |  |[default to false]

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dump_status

> models::RepositoryDumpStatus dump_status(repository, task_id)
Status of a repository dump task

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**task_id** | **String** |  | [required] |

### Return type

[**models::RepositoryDumpStatus**](RepositoryDumpStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dump_submit

> models::TaskInfo dump_submit(repository)
Backup the repository metadata (tags, commits, branches) and save the backup to the object store.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

[**models::TaskInfo**](TaskInfo.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_branch_protection_rules

> Vec<models::BranchProtectionRule> get_branch_protection_rules(repository)
get branch protection rules

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

[**Vec<models::BranchProtectionRule>**](BranchProtectionRule.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_gc_rules

> models::GarbageCollectionRules get_gc_rules(repository)
get repository GC rules

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

[**models::GarbageCollectionRules**](GarbageCollectionRules.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_repository

> models::Repository get_repository(repository)
get repository

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

[**models::Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_repository_metadata

> std::collections::HashMap<String, String> get_repository_metadata(repository)
get repository metadata

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

**std::collections::HashMap<String, String>**

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_repositories

> models::RepositoryList list_repositories(prefix, after, amount, search)
list repositories

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | Option<**String**> | return items prefixed with this value |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**search** | Option<**String**> | string for searching relevant entries |  |

### Return type

[**models::RepositoryList**](RepositoryList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## restore_status

> models::RepositoryRestoreStatus restore_status(repository, task_id)
Status of a restore request

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**task_id** | **String** |  | [required] |

### Return type

[**models::RepositoryRestoreStatus**](RepositoryRestoreStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## restore_submit

> models::TaskInfo restore_submit(repository, refs_restore)
Restore repository from a dump in the object store

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**refs_restore** | [**RefsRestore**](RefsRestore.md) |  | [required] |

### Return type

[**models::TaskInfo**](TaskInfo.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_branch_protection_rules

> set_branch_protection_rules(repository, branch_protection_rule, if_match)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch_protection_rule** | [**Vec<models::BranchProtectionRule>**](BranchProtectionRule.md) |  | [required] |
**if_match** | Option<**String**> | if provided, the branch protection rules will be updated only if the current ETag match the provided value |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_gc_rules

> set_gc_rules(repository, garbage_collection_rules)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**garbage_collection_rules** | [**GarbageCollectionRules**](GarbageCollectionRules.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

