# PrepareGarbageCollectionCommitsStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**task_id** | **String** | the id of the task preparing the GC commits | 
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **String** | last time the task status was updated | 
**result** | Option<[**models::GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md)> |  | [optional]
**error** | Option<[**models::Error**](Error.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


