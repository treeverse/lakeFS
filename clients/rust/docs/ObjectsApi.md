# \ObjectsApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**copy_object**](ObjectsApi.md#copy_object) | **POST** /repositories/{repository}/branches/{branch}/objects/copy | create a copy of an object
[**delete_object**](ObjectsApi.md#delete_object) | **DELETE** /repositories/{repository}/branches/{branch}/objects | delete object. Missing objects will not return a NotFound error.
[**delete_objects**](ObjectsApi.md#delete_objects) | **POST** /repositories/{repository}/branches/{branch}/objects/delete | delete objects. Missing objects will not return a NotFound error.
[**get_object**](ObjectsApi.md#get_object) | **GET** /repositories/{repository}/refs/{ref}/objects | get object content
[**get_underlying_properties**](ObjectsApi.md#get_underlying_properties) | **GET** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage
[**head_object**](ObjectsApi.md#head_object) | **HEAD** /repositories/{repository}/refs/{ref}/objects | check if object exists
[**list_objects**](ObjectsApi.md#list_objects) | **GET** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix
[**stat_object**](ObjectsApi.md#stat_object) | **GET** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata
[**upload_object**](ObjectsApi.md#upload_object) | **POST** /repositories/{repository}/branches/{branch}/objects | 



## copy_object

> models::ObjectStats copy_object(repository, branch, dest_path, object_copy_creation)
create a copy of an object

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** | destination branch for the copy | [required] |
**dest_path** | **String** | destination path relative to the branch | [required] |
**object_copy_creation** | [**ObjectCopyCreation**](ObjectCopyCreation.md) |  | [required] |

### Return type

[**models::ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_object

> delete_object(repository, branch, path, force)
delete object. Missing objects will not return a NotFound error.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |
**force** | Option<**bool**> |  |  |[default to false]

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_objects

> models::ObjectErrorList delete_objects(repository, branch, path_list, force)
delete objects. Missing objects will not return a NotFound error.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**path_list** | [**PathList**](PathList.md) |  | [required] |
**force** | Option<**bool**> |  |  |[default to false]

### Return type

[**models::ObjectErrorList**](ObjectErrorList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_object

> std::path::PathBuf get_object(repository, r#ref, path, range, if_none_match, presign)
get object content

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**r#ref** | **String** | a reference (could be either a branch or a commit ID) | [required] |
**path** | **String** | relative to the ref | [required] |
**range** | Option<**String**> | Byte range to retrieve |  |
**if_none_match** | Option<**String**> | Returns response only if the object does not have a matching ETag |  |
**presign** | Option<**bool**> |  |  |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream, application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_underlying_properties

> models::UnderlyingObjectProperties get_underlying_properties(repository, r#ref, path)
get object properties on underlying storage

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**r#ref** | **String** | a reference (could be either a branch or a commit ID) | [required] |
**path** | **String** | relative to the branch | [required] |

### Return type

[**models::UnderlyingObjectProperties**](UnderlyingObjectProperties.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## head_object

> head_object(repository, r#ref, path, range)
check if object exists

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**r#ref** | **String** | a reference (could be either a branch or a commit ID) | [required] |
**path** | **String** | relative to the ref | [required] |
**range** | Option<**String**> | Byte range to retrieve |  |

### Return type

 (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_objects

> models::ObjectStatsList list_objects(repository, r#ref, user_metadata, presign, after, amount, delimiter, prefix)
list objects under a given prefix

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**r#ref** | **String** | a reference (could be either a branch or a commit ID) | [required] |
**user_metadata** | Option<**bool**> |  |  |[default to true]
**presign** | Option<**bool**> |  |  |
**after** | Option<**String**> | return items after this value |  |
**amount** | Option<**i32**> | how many items to return |  |[default to 100]
**delimiter** | Option<**String**> | delimiter used to group common prefixes by |  |
**prefix** | Option<**String**> | return items prefixed with this value |  |

### Return type

[**models::ObjectStatsList**](ObjectStatsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stat_object

> models::ObjectStats stat_object(repository, r#ref, path, user_metadata, presign)
get object metadata

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**r#ref** | **String** | a reference (could be either a branch or a commit ID) | [required] |
**path** | **String** | relative to the branch | [required] |
**user_metadata** | Option<**bool**> |  |  |[default to true]
**presign** | Option<**bool**> |  |  |

### Return type

[**models::ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## upload_object

> models::ObjectStats upload_object(repository, branch, path, if_none_match, storage_class, force, content)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**repository** | **String** |  | [required] |
**branch** | **String** |  | [required] |
**path** | **String** | relative to the branch | [required] |
**if_none_match** | Option<**String**> | Set to \"*\" to atomically allow the upload only if the key has no object yet. Other values are not supported. |  |
**storage_class** | Option<**String**> | Deprecated, this capability will not be supported in future releases. |  |
**force** | Option<**bool**> |  |  |[default to false]
**content** | Option<**std::path::PathBuf**> | Only a single file per upload which must be named \\\"content\\\". |  |

### Return type

[**models::ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: multipart/form-data, application/octet-stream
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

