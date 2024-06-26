

# ObjectStats


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**path** | **String** |  |  |
|**pathType** | [**PathTypeEnum**](#PathTypeEnum) |  |  |
|**physicalAddress** | **String** | The location of the object on the underlying object store. Formatted as a native URI with the object store type as scheme (\&quot;s3://...\&quot;, \&quot;gs://...\&quot;, etc.) Or, in the case of presign&#x3D;true, will be an HTTP URL to be consumed via regular HTTP GET  |  |
|**physicalAddressExpiry** | **Long** | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  |  [optional] |
|**checksum** | **String** |  |  |
|**sizeBytes** | **Long** | The number of bytes in the object.  lakeFS always populates this field when returning ObjectStats.  This field is optional _for the client_ to supply, for instance on upload.  |  [optional] |
|**mtime** | **Long** | Unix Epoch in seconds |  |
|**metadata** | **Map&lt;String, String&gt;** |  |  [optional] |
|**contentType** | **String** | Object media type |  [optional] |



## Enum: PathTypeEnum

| Name | Value |
|---- | -----|
| COMMON_PREFIX | &quot;common_prefix&quot; |
| OBJECT | &quot;object&quot; |



