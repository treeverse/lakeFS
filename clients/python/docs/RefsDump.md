# RefsDump


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**commits_meta_range_id** | **str** |  | 
**tags_meta_range_id** | **str** |  | 
**branches_meta_range_id** | **str** |  | 

## Example

```python
from lakefs_sdk.models.refs_dump import RefsDump

# TODO update the JSON string below
json = "{}"
# create an instance of RefsDump from a JSON string
refs_dump_instance = RefsDump.from_json(json)
# print the JSON string representation of the object
print RefsDump.to_json()

# convert the object into a dict
refs_dump_dict = refs_dump_instance.to_dict()
# create an instance of RefsDump from a dict
refs_dump_form_dict = refs_dump.from_dict(refs_dump_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


