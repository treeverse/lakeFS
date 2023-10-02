# IngestRangeCreationResponse


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**range** | [**RangeMetadata**](RangeMetadata.md) |  | [optional] 
**pagination** | [**ImportPagination**](ImportPagination.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.ingest_range_creation_response import IngestRangeCreationResponse

# TODO update the JSON string below
json = "{}"
# create an instance of IngestRangeCreationResponse from a JSON string
ingest_range_creation_response_instance = IngestRangeCreationResponse.from_json(json)
# print the JSON string representation of the object
print IngestRangeCreationResponse.to_json()

# convert the object into a dict
ingest_range_creation_response_dict = ingest_range_creation_response_instance.to_dict()
# create an instance of IngestRangeCreationResponse from a dict
ingest_range_creation_response_form_dict = ingest_range_creation_response.from_dict(ingest_range_creation_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


