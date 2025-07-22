# BranchProtectionRule

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pattern** | **String** | fnmatch pattern for the branch name, supporting * and ? wildcards | 
**blocked_actions** | Option<**Vec<String>**> | List of actions to block on protected branches. If not specified, defaults to [\"staging_write\", \"commit\"] | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


