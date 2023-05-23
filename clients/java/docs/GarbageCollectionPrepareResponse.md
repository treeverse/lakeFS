

# GarbageCollectionPrepareResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**runId** | **String** | a unique identifier generated for this GC job | 
**gcCommitsLocation** | **String** | location of the resulting commits csv table (partitioned by run_id) | 
**gcAddressesLocation** | **String** | location to use for expired addresses parquet table (partitioned by run_id) | 
**gcCommitsPresignedUrl** | **String** | a presigned url to download the commits csv |  [optional]



