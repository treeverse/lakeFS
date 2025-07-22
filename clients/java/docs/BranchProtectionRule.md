

# BranchProtectionRule


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**pattern** | **String** | fnmatch pattern for the branch name, supporting * and ? wildcards |  |
|**blockedActions** | [**List&lt;BlockedActionsEnum&gt;**](#List&lt;BlockedActionsEnum&gt;) | List of actions to block on protected branches. If not specified, defaults to [\&quot;staging_write\&quot;, \&quot;commit\&quot;] |  [optional] |



## Enum: List&lt;BlockedActionsEnum&gt;

| Name | Value |
|---- | -----|
| STAGING_WRITE | &quot;staging_write&quot; |
| COMMIT | &quot;commit&quot; |
| DELETE | &quot;delete&quot; |



