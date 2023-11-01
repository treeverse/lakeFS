# GroupList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Group]**](Group.md) |  | 

## Example

```python
from lakefs_sdk.models.group_list import GroupList

# TODO update the JSON string below
json = "{}"
# create an instance of GroupList from a JSON string
group_list_instance = GroupList.from_json(json)
# print the JSON string representation of the object
print GroupList.to_json()

# convert the object into a dict
group_list_dict = group_list_instance.to_dict()
# create an instance of GroupList from a dict
group_list_form_dict = group_list.from_dict(group_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


