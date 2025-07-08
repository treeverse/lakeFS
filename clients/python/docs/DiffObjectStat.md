# DiffObjectStat


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**checksum** | **str** |  | 
**mtime** | **int** | Unix Epoch in seconds | 
**content_type** | **str** | Object media type | 
**metadata** | **Dict[str, str]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.diff_object_stat import DiffObjectStat

# TODO update the JSON string below
json = "{}"
# create an instance of DiffObjectStat from a JSON string
diff_object_stat_instance = DiffObjectStat.from_json(json)
# print the JSON string representation of the object
print DiffObjectStat.to_json()

# convert the object into a dict
diff_object_stat_dict = diff_object_stat_instance.to_dict()
# create an instance of DiffObjectStat from a dict
diff_object_stat_form_dict = diff_object_stat.from_dict(diff_object_stat_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


