# AsyncTaskStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**task_id** | **String** | the id of the async task | 
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **String** | last time the task status was updated | 
**error** | Option<[**models::Error**](Error.md)> |  | [optional]
**status_code** | Option<**i32**> | an http status code that correlates with the underlying error if exists | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


