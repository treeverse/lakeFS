# RefList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Ref]**](Ref.md) |  | 

## Example

```python
from lakefs_sdk.models.ref_list import RefList

# TODO update the JSON string below
json = "{}"
# create an instance of RefList from a JSON string
ref_list_instance = RefList.from_json(json)
# print the JSON string representation of the object
print RefList.to_json()

# convert the object into a dict
ref_list_dict = ref_list_instance.to_dict()
# create an instance of RefList from a dict
ref_list_form_dict = ref_list.from_dict(ref_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


