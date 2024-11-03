# StagingMetadata

information about uploaded object

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**staging** | [**StagingLocation**](StagingLocation.md) |  | 
**checksum** | **str** | unique identifier of object content on backing store (typically ETag) | 
**size_bytes** | **int** |  | 
**user_metadata** | **{str: (str,)}** |  | [optional] 
**content_type** | **str** | Object media type | [optional] 
**mtime** | **int** | Unix Epoch in seconds.  May be ignored by server. | [optional] 
**force** | **bool** |  | [optional]  if omitted the server will use the default value of False
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


