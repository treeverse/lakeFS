# ObjectStats

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**path** | **String** |  | 
**path_type** | **String** |  | 
**physical_address** | **String** | The location of the object on the underlying object store. Formatted as a native URI with the object store type as scheme (\"s3://...\", \"gs://...\", etc.) Or, in the case of presign=true, will be an HTTP URL to be consumed via regular HTTP GET  | 
**physical_address_expiry** | Option<**i64**> | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  | [optional]
**checksum** | **String** |  | 
**size_bytes** | Option<**i64**> |  | [optional]
**mtime** | **i64** | Unix Epoch in seconds | 
**metadata** | Option<**std::collections::HashMap<String, String>**> |  | [optional]
**content_type** | Option<**String**> | Object media type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


