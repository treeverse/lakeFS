

# GarbageCollectionPrepareResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**runId** | **String** | a unique identifier generated for this GC job | 
**gcCommitsLocation** | **String** | location of commits csv table (partitioned by run_id) | 
**gcAddressesLocation** | **String** | location for expired addresses parquet table (partitioned by run_id) | 



