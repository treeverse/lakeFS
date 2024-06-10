# \InternalApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_branch_protection_rule_preflight**](InternalApi.md#create_branch_protection_rule_preflight) | **GET** /repositories/{repository}/branch_protection/set_allowed | 
[**create_commit_record**](InternalApi.md#create_commit_record) | **POST** /repositories/{repository}/commits | create commit record
[**create_symlink_file**](InternalApi.md#create_symlink_file) | **POST** /repositories/{repository}/refs/{branch}/symlink | creates symlink files corresponding to the given directory
[**delete_repository_metadata**](InternalApi.md#delete_repository_metadata) | **DELETE** /repositories/{repository}/metadata | delete repository metadata
[**dump_refs**](InternalApi.md#dump_refs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs (tags, commits, branches) to object store Deprecated: a new API will introduce long running operations 
[**get_auth_capabilities**](InternalApi.md#get_auth_capabilities) | **GET** /auth/capabilities | list authentication capabilities supported
[**get_garbage_collection_config**](InternalApi.md#get_garbage_collection_config) | **GET** /config/garbage-collection | 
[**get_lake_fs_version**](InternalApi.md#get_lake_fs_version) | **GET** /config/version | 
[**get_metadata_object**](InternalApi.md#get_metadata_object) | **GET** /repositories/{repository}/metadata/object/{type}/{object_id} | return a lakeFS metadata object by ID
[**get_setup_state**](InternalApi.md#get_setup_state) | **GET** /setup_lakefs | check if the lakeFS installation is already set up
[**get_storage_config**](InternalApi.md#get_storage_config) | **GET** /config/storage | 
[**get_usage_report_summary**](InternalApi.md#get_usage_report_summary) | **GET** /usage-report/summary | get usage report summary
[**internal_create_branch_protection_rule**](InternalApi.md#internal_create_branch_protection_rule) | **POST** /repositories/{repository}/branch_protection | 
[**internal_delete_branch_protection_rule**](InternalApi.md#internal_delete_branch_protection_rule) | **DELETE** /repositories/{repository}/branch_protection | 
[**internal_delete_garbage_collection_rules**](InternalApi.md#internal_delete_garbage_collection_rules) | **DELETE** /repositories/{repository}/gc/rules | 
[**internal_get_branch_protection_rules**](InternalApi.md#internal_get_branch_protection_rules) | **GET** /repositories/{repository}/branch_protection | get branch protection rules
[**internal_get_garbage_collection_rules**](InternalApi.md#internal_get_garbage_collection_rules) | **GET** /repositories/{repository}/gc/rules | 
[**internal_set_garbage_collection_rules**](InternalApi.md#internal_set_garbage_collection_rules) | **POST** /repositories/{repository}/gc/rules | 
[**post_stats_events**](InternalApi.md#post_stats_events) | **POST** /statistics | post stats events, this endpoint is meant for internal use only
[**prepare_garbage_collection_commits**](InternalApi.md#prepare_garbage_collection_commits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active commits for garbage collection
[**prepare_garbage_collection_uncommitted**](InternalApi.md#prepare_garbage_collection_uncommitted) | **POST** /repositories/{repository}/gc/prepare_uncommited | save repository uncommitted metadata for garbage collection
[**restore_refs**](InternalApi.md#restore_refs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs (tags, commits, branches) from object store. Deprecated: a new API will introduce long running operations 
[**set_garbage_collection_rules_preflight**](InternalApi.md#set_garbage_collection_rules_preflight) | **GET** /repositories/{repository}/gc/rules/set_allowed | 
[**set_repository_metadata**](InternalApi.md#set_repository_metadata) | **POST** /repositories/{repository}/metadata | set repository metadata
[**setup**](InternalApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user
[**setup_comm_prefs**](InternalApi.md#setup_comm_prefs) | **POST** /setup_comm_prefs | setup communications preferences
[**stage_object**](InternalApi.md#stage_object) | **PUT** /repositories/{repository}/branches/{branch}/objects | stage an object's metadata for the given branch
[**upload_object_preflight**](InternalApi.md#upload_object_preflight) | **GET** /repositories/{repository}/branches/{branch}/objects/stage_allowed | 



## create_branch_protection_rule_preflight

> create_branch_protection_rule_preflight(repository)


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


## create_commit_record

> create_commit_record(repository, commit_record_creation)
create commit record

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**commit_record_creation** | [**CommitRecordCreation**](CommitRecordCreation.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_symlink_file

> models::StorageUri create_symlink_file(repository, branch, location)
creates symlink files corresponding to the given directory

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**location** | Option<**String**> | path to the table data |  |

### Return type

[**models::StorageUri**](StorageURI.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_repository_metadata

> delete_repository_metadata(repository, repository_metadata_keys)
delete repository metadata

Delete specified keys from the repository's metadata. 

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**repository_metadata_keys** | [**RepositoryMetadataKeys**](RepositoryMetadataKeys.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dump_refs

> models::RefsDump dump_refs(repository)
Dump repository refs (tags, commits, branches) to object store Deprecated: a new API will introduce long running operations 

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

[**models::RefsDump**](RefsDump.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_auth_capabilities

> models::AuthCapabilities get_auth_capabilities()
list authentication capabilities supported

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::AuthCapabilities**](AuthCapabilities.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_garbage_collection_config

> models::GarbageCollectionConfig get_garbage_collection_config()


get information of gc settings

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::GarbageCollectionConfig**](GarbageCollectionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_lake_fs_version

> models::VersionConfig get_lake_fs_version()


get version of lakeFS server

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::VersionConfig**](VersionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_metadata_object

> std::path::PathBuf get_metadata_object(repository, object_id, r#type, presign)
return a lakeFS metadata object by ID

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**object_id** | **String** |  | [required] |
**r#type** | **String** |  | [required] |
**presign** | Option<**bool**> |  |  |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream, application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_setup_state

> models::SetupState get_setup_state()
check if the lakeFS installation is already set up

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::SetupState**](SetupState.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_storage_config

> models::StorageConfig get_storage_config()


retrieve lakeFS storage configuration

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::StorageConfig**](StorageConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_usage_report_summary

> models::InstallationUsageReport get_usage_report_summary()
get usage report summary

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::InstallationUsageReport**](InstallationUsageReport.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json, application/text

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## internal_create_branch_protection_rule

> internal_create_branch_protection_rule(repository, branch_protection_rule)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch_protection_rule** | [**BranchProtectionRule**](BranchProtectionRule.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## internal_delete_branch_protection_rule

> internal_delete_branch_protection_rule(repository, internal_delete_branch_protection_rule_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**internal_delete_branch_protection_rule_request** | [**InternalDeleteBranchProtectionRuleRequest**](InternalDeleteBranchProtectionRuleRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## internal_delete_garbage_collection_rules

> internal_delete_garbage_collection_rules(repository)


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


## internal_get_branch_protection_rules

> Vec<models::BranchProtectionRule> internal_get_branch_protection_rules(repository)
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


## internal_get_garbage_collection_rules

> models::GarbageCollectionRules internal_get_garbage_collection_rules(repository)


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


## internal_set_garbage_collection_rules

> internal_set_garbage_collection_rules(repository, garbage_collection_rules)


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


## post_stats_events

> post_stats_events(stats_events_list)
post stats events, this endpoint is meant for internal use only

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**stats_events_list** | [**StatsEventsList**](StatsEventsList.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## prepare_garbage_collection_commits

> models::GarbageCollectionPrepareResponse prepare_garbage_collection_commits(repository)
save lists of active commits for garbage collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |

### Return type

[**models::GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## prepare_garbage_collection_uncommitted

> models::PrepareGcUncommittedResponse prepare_garbage_collection_uncommitted(repository, prepare_gc_uncommitted_request)
save repository uncommitted metadata for garbage collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**prepare_gc_uncommitted_request** | Option<[**PrepareGcUncommittedRequest**](PrepareGcUncommittedRequest.md)> |  |  |

### Return type

[**models::PrepareGcUncommittedResponse**](PrepareGCUncommittedResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## restore_refs

> restore_refs(repository, refs_restore)
Restore repository refs (tags, commits, branches) from object store. Deprecated: a new API will introduce long running operations 

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**refs_restore** | [**RefsRestore**](RefsRestore.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_garbage_collection_rules_preflight

> set_garbage_collection_rules_preflight(repository)


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


## set_repository_metadata

> set_repository_metadata(repository, repository_metadata_set)
set repository metadata

Set repository metadata. This will only add or update the provided keys, and will not remove any existing keys. 

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**repository_metadata_set** | [**RepositoryMetadataSet**](RepositoryMetadataSet.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## setup

> models::CredentialsWithSecret setup(setup)
setup lakeFS and create a first user

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**setup** | [**Setup**](Setup.md) |  | [required] |

### Return type

[**models::CredentialsWithSecret**](CredentialsWithSecret.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## setup_comm_prefs

> setup_comm_prefs(comm_prefs_input)
setup communications preferences

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**comm_prefs_input** | [**CommPrefsInput**](CommPrefsInput.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stage_object

> models::ObjectStats stage_object(repository, branch, path, object_stage_creation)
stage an object's metadata for the given branch

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |
**object_stage_creation** | [**ObjectStageCreation**](ObjectStageCreation.md) |  | [required] |

### Return type

[**models::ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## upload_object_preflight

> upload_object_preflight(repository, branch, path)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

