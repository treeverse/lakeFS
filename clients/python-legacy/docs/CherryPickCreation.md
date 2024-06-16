# CherryPickCreation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ref** | **str** | the commit to cherry-pick, given by a ref | 
**parent_number** | **int** | When cherry-picking a merge commit, the parent number (starting from 1) with which to perform the diff. The default branch is parent 1.  | [optional] 
**commit_overrides** | [**CommitOverrides**](CommitOverrides.md) |  | [optional] 
**force** | **bool** |  | [optional]  if omitted the server will use the default value of False
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


