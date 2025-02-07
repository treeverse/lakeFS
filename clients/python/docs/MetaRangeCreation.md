# MetaRangeCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ranges** | [**List[RangeMetadata]**](RangeMetadata.md) |  | 

## Example

```python
from lakefs_sdk.models.meta_range_creation import MetaRangeCreation

# TODO update the JSON string below
json = "{}"
# create an instance of MetaRangeCreation from a JSON string
meta_range_creation_instance = MetaRangeCreation.from_json(json)
# print the JSON string representation of the object
print MetaRangeCreation.to_json()

# convert the object into a dict
meta_range_creation_dict = meta_range_creation_instance.to_dict()
# create an instance of MetaRangeCreation from a dict
meta_range_creation_form_dict = meta_range_creation.from_dict(meta_range_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


