# ObjectStats


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**path** | **str** |  | 
**path_type** | **str** |  | 
**physical_address** | **str** | The location of the object on the underlying object store. Formatted as a native URI with the object store type as scheme (\&quot;s3://...\&quot;, \&quot;gs://...\&quot;, etc.) Or, in the case of presign&#x3D;true, will be an HTTP URL to be consumed via regular HTTP GET  | 
**checksum** | **str** |  | 
**mtime** | **int** | Unix Epoch in seconds | 
**size_bytes** | **int** |  | [optional] 
**metadata** | [**ObjectUserMetadata**](ObjectUserMetadata.md) |  | [optional] 
**content_type** | **str** | Object media type | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


