# RefsDumpStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the task | 
**completed** | **bool** |  | 
**update_time** | **datetime** |  | 
**error** | **str** |  | [optional] 
**refs** | [**RefsDump**](RefsDump.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.refs_dump_status import RefsDumpStatus

# TODO update the JSON string below
json = "{}"
# create an instance of RefsDumpStatus from a JSON string
refs_dump_status_instance = RefsDumpStatus.from_json(json)
# print the JSON string representation of the object
print RefsDumpStatus.to_json()

# convert the object into a dict
refs_dump_status_dict = refs_dump_status_instance.to_dict()
# create an instance of RefsDumpStatus from a dict
refs_dump_status_form_dict = refs_dump_status.from_dict(refs_dump_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


