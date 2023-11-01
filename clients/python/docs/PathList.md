# PathList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**paths** | **List[str]** |  | 

## Example

```python
from lakefs_sdk.models.path_list import PathList

# TODO update the JSON string below
json = "{}"
# create an instance of PathList from a JSON string
path_list_instance = PathList.from_json(json)
# print the JSON string representation of the object
print PathList.to_json()

# convert the object into a dict
path_list_dict = path_list_instance.to_dict()
# create an instance of PathList from a dict
path_list_form_dict = path_list.from_dict(path_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


