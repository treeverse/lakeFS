# ImportStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**completed** | **bool** |  | 
**update_time** | **datetime** |  | 
**ingested_objects** | **int** | Number of objects processed so far | [optional] 
**metarange_id** | **str** |  | [optional] 
**commit** | [**Commit**](Commit.md) |  | [optional] 
**error** | [**Error**](Error.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.import_status import ImportStatus

# TODO update the JSON string below
json = "{}"
# create an instance of ImportStatus from a JSON string
import_status_instance = ImportStatus.from_json(json)
# print the JSON string representation of the object
print(ImportStatus.to_json())

# convert the object into a dict
import_status_dict = import_status_instance.to_dict()
# create an instance of ImportStatus from a dict
import_status_from_dict = ImportStatus.from_dict(import_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


