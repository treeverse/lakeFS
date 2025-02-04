# RepositoryRestoreStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the task | 
**done** | **bool** |  | 
**update_time** | **datetime** |  | 
**error** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.repository_restore_status import RepositoryRestoreStatus

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryRestoreStatus from a JSON string
repository_restore_status_instance = RepositoryRestoreStatus.from_json(json)
# print the JSON string representation of the object
print(RepositoryRestoreStatus.to_json())

# convert the object into a dict
repository_restore_status_dict = repository_restore_status_instance.to_dict()
# create an instance of RepositoryRestoreStatus from a dict
repository_restore_status_from_dict = RepositoryRestoreStatus.from_dict(repository_restore_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


