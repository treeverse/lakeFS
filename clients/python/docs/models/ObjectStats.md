# lakefs_client.model.object_stats.ObjectStats

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**physical_address** | str,  | str,  | The location of the object on the underlying object store. Formatted as a native URI with the object store type as scheme (\&quot;s3://...\&quot;, \&quot;gs://...\&quot;, etc.) Or, in the case of presign&#x3D;true, will be an HTTP URL to be consumed via regular HTTP GET  | 
**path** | str,  | str,  |  | 
**checksum** | str,  | str,  |  | 
**path_type** | str,  | str,  |  | must be one of ["common_prefix", "object", ] 
**mtime** | decimal.Decimal, int,  | decimal.Decimal,  | Unix Epoch in seconds | value must be a 64 bit integer
**size_bytes** | decimal.Decimal, int,  | decimal.Decimal,  |  | [optional] value must be a 64 bit integer
**metadata** | [**ObjectUserMetadata**](ObjectUserMetadata.md) | [**ObjectUserMetadata**](ObjectUserMetadata.md) |  | [optional] 
**content_type** | str,  | str,  | Object media type | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

