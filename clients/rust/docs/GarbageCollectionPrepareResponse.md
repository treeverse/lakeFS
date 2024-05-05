# GarbageCollectionPrepareResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**run_id** | **String** | a unique identifier generated for this GC job | 
**gc_commits_location** | **String** | location of the resulting commits csv table (partitioned by run_id) | 
**gc_addresses_location** | **String** | location to use for expired addresses parquet table (partitioned by run_id) | 
**gc_commits_presigned_url** | Option<**String**> | a presigned url to download the commits csv | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


