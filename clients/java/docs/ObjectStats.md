

# ObjectStats


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**path** | **String** |  | 
**pathType** | [**PathTypeEnum**](#PathTypeEnum) |  | 
**physicalAddress** | **String** | The location of the object on the underlying object store. Formatted as a native URI with the object store type as scheme (\&quot;s3://...\&quot;, \&quot;gs://...\&quot;, etc.) Or, in the case of presign&#x3D;true, will be an HTTP URL to be consumed via regular HTTP GET  | 
**checksum** | **String** |  | 
**sizeBytes** | **Long** |  |  [optional]
**mtime** | **Long** | Unix Epoch in seconds | 
**metadata** | **Map&lt;String, String&gt;** |  |  [optional]
**contentType** | **String** | Object media type |  [optional]



## Enum: PathTypeEnum

Name | Value
---- | -----
COMMON_PREFIX | &quot;common_prefix&quot;
OBJECT | &quot;object&quot;



