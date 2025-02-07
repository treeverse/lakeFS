# GroupCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**description** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.group_creation import GroupCreation

# TODO update the JSON string below
json = "{}"
# create an instance of GroupCreation from a JSON string
group_creation_instance = GroupCreation.from_json(json)
# print the JSON string representation of the object
print GroupCreation.to_json()

# convert the object into a dict
group_creation_dict = group_creation_instance.to_dict()
# create an instance of GroupCreation from a dict
group_creation_form_dict = group_creation.from_dict(group_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


