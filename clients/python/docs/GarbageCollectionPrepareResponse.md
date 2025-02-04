# GarbageCollectionPrepareResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**run_id** | **str** | a unique identifier generated for this GC job | 
**gc_commits_location** | **str** | location of the resulting commits csv table (partitioned by run_id) | 
**gc_addresses_location** | **str** | location to use for expired addresses parquet table (partitioned by run_id) | 
**gc_commits_presigned_url** | **str** | a presigned url to download the commits csv | [optional] 

## Example

```python
from lakefs_sdk.models.garbage_collection_prepare_response import GarbageCollectionPrepareResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GarbageCollectionPrepareResponse from a JSON string
garbage_collection_prepare_response_instance = GarbageCollectionPrepareResponse.from_json(json)
# print the JSON string representation of the object
print(GarbageCollectionPrepareResponse.to_json())

# convert the object into a dict
garbage_collection_prepare_response_dict = garbage_collection_prepare_response_instance.to_dict()
# create an instance of GarbageCollectionPrepareResponse from a dict
garbage_collection_prepare_response_from_dict = GarbageCollectionPrepareResponse.from_dict(garbage_collection_prepare_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


