# RangeMetadata


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the range. | 
**min_key** | **str** | First key in the range. | 
**max_key** | **str** | Last key in the range. | 
**count** | **int** | Number of records in the range. | 
**estimated_size** | **int** | Estimated size of the range in bytes | 

## Example

```python
from lakefs_sdk.models.range_metadata import RangeMetadata

# TODO update the JSON string below
json = "{}"
# create an instance of RangeMetadata from a JSON string
range_metadata_instance = RangeMetadata.from_json(json)
# print the JSON string representation of the object
print(RangeMetadata.to_json())

# convert the object into a dict
range_metadata_dict = range_metadata_instance.to_dict()
# create an instance of RangeMetadata from a dict
range_metadata_from_dict = RangeMetadata.from_dict(range_metadata_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


