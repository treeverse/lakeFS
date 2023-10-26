

# ImportLocation


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**type** | [**TypeEnum**](#TypeEnum) | Path type, can either be &#39;common_prefix&#39; or &#39;object&#39; |  |
|**path** | **String** | A source location to a prefix or to a single object. Must match the lakeFS installation blockstore type. |  |
|**destination** | **String** | Destination for the imported objects on the branch. Must be a relative path to the branch. If the type is &#39;object&#39;, the destination is the exact object name under the branch. If the type is &#39;common_prefix&#39;, the destination is the prefix under the branch.  |  |



## Enum: TypeEnum

| Name | Value |
|---- | -----|
| COMMON_PREFIX | &quot;common_prefix&quot; |
| OBJECT | &quot;object&quot; |



