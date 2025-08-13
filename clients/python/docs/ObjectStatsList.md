# ObjectStatsList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[ObjectStats]**](ObjectStats.md) |  | 

## Example

```python
from lakefs_sdk.models.object_stats_list import ObjectStatsList

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectStatsList from a JSON string
object_stats_list_instance = ObjectStatsList.from_json(json)
# print the JSON string representation of the object
print ObjectStatsList.to_json()

# convert the object into a dict
object_stats_list_dict = object_stats_list_instance.to_dict()
# create an instance of ObjectStatsList from a dict
object_stats_list_form_dict = object_stats_list.from_dict(object_stats_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


