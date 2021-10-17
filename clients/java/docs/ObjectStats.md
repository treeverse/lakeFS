

# ObjectStats


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**path** | **String** |  | 
**pathType** | [**PathTypeEnum**](#PathTypeEnum) |  | 
**physicalAddress** | **String** |  | 
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



