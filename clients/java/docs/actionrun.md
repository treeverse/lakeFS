# ActionRun

## Properties

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **runId** | **String** |  |  |
| **branch** | **String** |  |  |
| **startTime** | **OffsetDateTime** |  |  |
| **endTime** | **OffsetDateTime** |  | \[optional\] |
| **eventType** | [**EventTypeEnum**](actionrun.md#EventTypeEnum) |  |  |
| **status** | [**StatusEnum**](actionrun.md#StatusEnum) |  |  |
| **commitId** | **String** |  |  |

## Enum: EventTypeEnum

| Name | Value |
| :--- | :--- |
| COMMIT | "pre\_commit" |
| MERGE | "pre\_merge" |

## Enum: StatusEnum

| Name | Value |
| :--- | :--- |
| FAILED | "failed" |
| COMPLETED | "completed" |

