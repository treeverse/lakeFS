

# Diff


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**type** | [**TypeEnum**](#TypeEnum) |  |  |
|**path** | **String** |  |  |
|**pathType** | [**PathTypeEnum**](#PathTypeEnum) |  |  |
|**sizeBytes** | **Long** | represents the size of the added/changed/deleted entry |  [optional] |



## Enum: TypeEnum

| Name | Value |
|---- | -----|
| ADDED | &quot;added&quot; |
| REMOVED | &quot;removed&quot; |
| CHANGED | &quot;changed&quot; |
| CONFLICT | &quot;conflict&quot; |
| PREFIX_CHANGED | &quot;prefix_changed&quot; |



## Enum: PathTypeEnum

| Name | Value |
|---- | -----|
| COMMON_PREFIX | &quot;common_prefix&quot; |
| OBJECT | &quot;object&quot; |



