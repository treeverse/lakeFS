# RevertCreation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ref** | **str** | the commit to revert, given by a ref | 
**parent_number** | **int** | when reverting a merge commit, the parent number (starting from 1) relative to which to perform the revert. | 
**force** | **bool** |  | [optional]  if omitted the server will use the default value of False
**allow_empty** | **bool** | allow empty commit (revert without changes) | [optional]  if omitted the server will use the default value of False
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


