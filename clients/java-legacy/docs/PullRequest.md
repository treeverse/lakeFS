

# PullRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** |  | 
**creationDate** | **Long** |  | 
**author** | **String** |  | 
**sourceBranch** | **String** |  | 
**destinationBranch** | **String** |  | 
**commitId** | **String** | the commit id of merged PRs |  [optional]
**status** | [**StatusEnum**](#StatusEnum) |  | 
**title** | **String** |  | 
**description** | **String** |  | 



## Enum: StatusEnum

Name | Value
---- | -----
OPEN | &quot;open&quot;
CLOSED | &quot;closed&quot;
MERGED | &quot;merged&quot;



