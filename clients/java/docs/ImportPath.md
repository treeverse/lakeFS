

# ImportPath


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**path** | **String** | A source location to ingested path or to a single object. Must match the lakeFS installation blockstore type. | 
**destination** | **String** | Destination for the imported objects on the branch | 
**type** | [**TypeEnum**](#TypeEnum) | Path type, can either be &#39;prefix&#39; or &#39;object&#39; | 



## Enum: TypeEnum

Name | Value
---- | -----
PREFIX | &quot;prefix&quot;
OBJECT | &quot;object&quot;



