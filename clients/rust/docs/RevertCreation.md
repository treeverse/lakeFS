# RevertCreation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#ref** | **String** | the commit to revert, given by a ref | 
**parent_number** | **i32** | when reverting a merge commit, the parent number (starting from 1) relative to which to perform the revert. | 
**force** | Option<**bool**> |  | [optional][default to false]
**allow_empty** | Option<**bool**> | allow empty commit (revert without changes) | [optional][default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


