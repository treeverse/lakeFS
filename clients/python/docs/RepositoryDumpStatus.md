# RepositoryDumpStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of the task | 
**done** | **bool** |  | 
**update_time** | **datetime** |  | 
**error** | **str** |  | [optional] 
**refs** | [**RefsDump**](RefsDump.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.repository_dump_status import RepositoryDumpStatus

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryDumpStatus from a JSON string
repository_dump_status_instance = RepositoryDumpStatus.from_json(json)
# print the JSON string representation of the object
print(RepositoryDumpStatus.to_json())

# convert the object into a dict
repository_dump_status_dict = repository_dump_status_instance.to_dict()
# create an instance of RepositoryDumpStatus from a dict
repository_dump_status_from_dict = RepositoryDumpStatus.from_dict(repository_dump_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


