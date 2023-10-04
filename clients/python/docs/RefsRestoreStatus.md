# RefsRestoreStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the task | 
**completed** | **bool** |  | 
**error** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.refs_restore_status import RefsRestoreStatus

# TODO update the JSON string below
json = "{}"
# create an instance of RefsRestoreStatus from a JSON string
refs_restore_status_instance = RefsRestoreStatus.from_json(json)
# print the JSON string representation of the object
print RefsRestoreStatus.to_json()

# convert the object into a dict
refs_restore_status_dict = refs_restore_status_instance.to_dict()
# create an instance of RefsRestoreStatus from a dict
refs_restore_status_form_dict = refs_restore_status.from_dict(refs_restore_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


