# StagingMetadata

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**staging** | [**models::StagingLocation**](StagingLocation.md) |  | 
**checksum** | **String** | unique identifier of object content on backing store (typically ETag) | 
**size_bytes** | **i64** |  | 
**user_metadata** | Option<**std::collections::HashMap<String, String>**> |  | [optional]
**content_type** | Option<**String**> | Object media type | [optional]
**force** | Option<**bool**> |  | [optional][default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


