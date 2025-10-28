# LocalTable


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **str** | Remote table namespace | 
**table** | **str** | Remote table name | 
**repository_id** | **str** | lakeFS repository ID | 
**reference_id** | **str** | lakeFS reference ID (branch or commit) | 

## Example

```python
from lakefs_sdk.models.local_table import LocalTable

# TODO update the JSON string below
json = "{}"
# create an instance of LocalTable from a JSON string
local_table_instance = LocalTable.from_json(json)
# print the JSON string representation of the object
print LocalTable.to_json()

# convert the object into a dict
local_table_dict = local_table_instance.to_dict()
# create an instance of LocalTable from a dict
local_table_form_dict = local_table.from_dict(local_table_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


