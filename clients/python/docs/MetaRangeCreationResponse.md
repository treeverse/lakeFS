# MetaRangeCreationResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the created metarange | [optional] 

## Example

```python
from lakefs_sdk.models.meta_range_creation_response import MetaRangeCreationResponse

# TODO update the JSON string below
json = "{}"
# create an instance of MetaRangeCreationResponse from a JSON string
meta_range_creation_response_instance = MetaRangeCreationResponse.from_json(json)
# print the JSON string representation of the object
print MetaRangeCreationResponse.to_json()

# convert the object into a dict
meta_range_creation_response_dict = meta_range_creation_response_instance.to_dict()
# create an instance of MetaRangeCreationResponse from a dict
meta_range_creation_response_form_dict = meta_range_creation_response.from_dict(meta_range_creation_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


