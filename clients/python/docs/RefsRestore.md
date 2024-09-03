# RefsRestore


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**commits_meta_range_id** | **str** |  | 
**tags_meta_range_id** | **str** |  | 
**branches_meta_range_id** | **str** |  | 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.refs_restore import RefsRestore

# TODO update the JSON string below
json = "{}"
# create an instance of RefsRestore from a JSON string
refs_restore_instance = RefsRestore.from_json(json)
# print the JSON string representation of the object
print(RefsRestore.to_json())

# convert the object into a dict
refs_restore_dict = refs_restore_instance.to_dict()
# create an instance of RefsRestore from a dict
refs_restore_from_dict = RefsRestore.from_dict(refs_restore_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


