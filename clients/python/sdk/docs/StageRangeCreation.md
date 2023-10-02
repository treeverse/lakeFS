# StageRangeCreation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**from_source_uri** | **str** | The source location of the ingested files. Must match the lakeFS installation blockstore type. | 
**after** | **str** | Only objects after this key would be ingested. | 
**prepend** | **str** | A prefix to prepend to ingested objects. | 
**continuation_token** | **str** | Opaque. Client should pass the continuation_token received from server to continue creation ranges from the same key. | [optional] 
**staging_token** | **str** | Opaque. Client should pass staging_token if received from server on previous request | [optional] 

## Example

```python
from lakefs_sdk.models.stage_range_creation import StageRangeCreation

# TODO update the JSON string below
json = "{}"
# create an instance of StageRangeCreation from a JSON string
stage_range_creation_instance = StageRangeCreation.from_json(json)
# print the JSON string representation of the object
print StageRangeCreation.to_json()

# convert the object into a dict
stage_range_creation_dict = stage_range_creation_instance.to_dict()
# create an instance of StageRangeCreation from a dict
stage_range_creation_form_dict = stage_range_creation.from_dict(stage_range_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


