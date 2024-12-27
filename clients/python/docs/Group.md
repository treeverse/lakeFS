# Group


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**name** | **str** |  | [optional] 
**description** | **str** |  | [optional] 
**creation_date** | **int** | Unix Epoch in seconds | 

## Example

```python
from lakefs_sdk.models.group import Group

# TODO update the JSON string below
json = "{}"
# create an instance of Group from a JSON string
group_instance = Group.from_json(json)
# print the JSON string representation of the object
print Group.to_json()

# convert the object into a dict
group_dict = group_instance.to_dict()
# create an instance of Group from a dict
group_form_dict = group.from_dict(group_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


