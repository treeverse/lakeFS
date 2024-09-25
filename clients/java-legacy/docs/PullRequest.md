

# PullRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status** | [**StatusEnum**](#StatusEnum) |  | 
**title** | **String** |  | 
**description** | **String** |  | 
**id** | **String** |  | 
**creationDate** | **OffsetDateTime** |  | 
**author** | **String** |  | 
**sourceBranch** | **String** |  | 
**destinationBranch** | **String** |  | 
**mergedCommitId** | **String** | the commit id of merged PRs |  [optional]
**closedDate** | **OffsetDateTime** |  |  [optional]



## Enum: StatusEnum

Name | Value
---- | -----
OPEN | &quot;open&quot;
CLOSED | &quot;closed&quot;
MERGED | &quot;merged&quot;



