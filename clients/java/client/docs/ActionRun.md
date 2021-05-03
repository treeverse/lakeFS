

# ActionRun


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**runId** | **String** |  | 
**branch** | **String** |  | 
**startTime** | **OffsetDateTime** |  | 
**endTime** | **OffsetDateTime** |  |  [optional]
**eventType** | [**EventTypeEnum**](#EventTypeEnum) |  | 
**status** | [**StatusEnum**](#StatusEnum) |  | 
**commitId** | **String** |  | 



## Enum: EventTypeEnum

Name | Value
---- | -----
COMMIT | &quot;pre_commit&quot;
MERGE | &quot;pre_merge&quot;



## Enum: StatusEnum

Name | Value
---- | -----
FAILED | &quot;failed&quot;
COMPLETED | &quot;completed&quot;



