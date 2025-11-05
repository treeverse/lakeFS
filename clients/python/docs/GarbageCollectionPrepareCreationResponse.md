# GarbageCollectionPrepareCreationResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the GC prepare task | 

## Example

```python
from lakefs_sdk.models.garbage_collection_prepare_creation_response import GarbageCollectionPrepareCreationResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GarbageCollectionPrepareCreationResponse from a JSON string
garbage_collection_prepare_creation_response_instance = GarbageCollectionPrepareCreationResponse.from_json(json)
# print the JSON string representation of the object
print GarbageCollectionPrepareCreationResponse.to_json()

# convert the object into a dict
garbage_collection_prepare_creation_response_dict = garbage_collection_prepare_creation_response_instance.to_dict()
# create an instance of GarbageCollectionPrepareCreationResponse from a dict
garbage_collection_prepare_creation_response_form_dict = garbage_collection_prepare_creation_response.from_dict(garbage_collection_prepare_creation_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


