# GarbageCollectionPrepareResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**run_id** | **String** | a unique identifier generated for this GC job | 
**gc_commits_location** | Option<**String**> | location of the resulting commits csv table (partitioned by run_id) | [optional]
**gc_addresses_location** | Option<**String**> | location to use for expired addresses parquet table (partitioned by run_id) | [optional]
**gc_commits_presigned_url** | Option<**String**> | a presigned url to download the commits csv | [optional]
**completed** | **bool** | true if the task has completed (either successfully or with an error) | 
**update_time** | **String** | last time the task status was updated | 
**error** | Option<[**models::Error**](Error.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


