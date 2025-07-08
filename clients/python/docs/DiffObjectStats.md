# DiffObjectStats


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**checksum** | **str** |  | 
**mtime** | **int** | Unix Epoch in seconds | 
**content_type** | **str** | Object media type | 
**metadata** | **Dict[str, str]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.diff_object_stats import DiffObjectStats

# TODO update the JSON string below
json = "{}"
# create an instance of DiffObjectStats from a JSON string
diff_object_stats_instance = DiffObjectStats.from_json(json)
# print the JSON string representation of the object
print DiffObjectStats.to_json()

# convert the object into a dict
diff_object_stats_dict = diff_object_stats_instance.to_dict()
# create an instance of DiffObjectStats from a dict
diff_object_stats_form_dict = diff_object_stats.from_dict(diff_object_stats_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


