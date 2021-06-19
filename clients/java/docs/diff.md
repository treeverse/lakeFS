# Diff

## Properties

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **type** | [**TypeEnum**](diff.md#TypeEnum) |  |  |
| **path** | **String** |  |  |
| **pathType** | [**PathTypeEnum**](diff.md#PathTypeEnum) |  |  |
| **sizeBytes** | **Long** | represents the size of the added/changed/deleted entry | \[optional\] |

## Enum: TypeEnum

| Name | Value |
| :--- | :--- |
| ADDED | "added" |
| REMOVED | "removed" |
| CHANGED | "changed" |
| CONFLICT | "conflict" |

## Enum: PathTypeEnum

| Name | Value |
| :--- | :--- |
| COMMON\_PREFIX | "common\_prefix" |
| OBJECT | "object" |

