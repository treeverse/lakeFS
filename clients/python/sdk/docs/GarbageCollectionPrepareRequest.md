# GarbageCollectionPrepareRequest


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**previous_run_id** | **str** | run id of a previous successful GC job | [optional] 

## Example

```python
from lakefs_sdk.models.garbage_collection_prepare_request import GarbageCollectionPrepareRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GarbageCollectionPrepareRequest from a JSON string
garbage_collection_prepare_request_instance = GarbageCollectionPrepareRequest.from_json(json)
# print the JSON string representation of the object
print GarbageCollectionPrepareRequest.to_json()

# convert the object into a dict
garbage_collection_prepare_request_dict = garbage_collection_prepare_request_instance.to_dict()
# create an instance of GarbageCollectionPrepareRequest from a dict
garbage_collection_prepare_request_form_dict = garbage_collection_prepare_request.from_dict(garbage_collection_prepare_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


