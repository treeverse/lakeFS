# GarbageCollectionPrepareResponse


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**run_id** | **str** | a unique identifier generated for this GC job | 
**gc_commits_location** | **str** | location of the resulting commits csv table (partitioned by run_id) | 
**gc_addresses_location** | **str** | location to use for expired addresses parquet table (partitioned by run_id) | 
**gc_commits_presigned_url** | **str** | a presigned url to download the commits csv | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


