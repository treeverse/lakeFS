# IcebergLocalTable


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** | Reference to one or more levels of a namespace | 
**table** | **str** | Remote table name | 
**repository_id** | **str** | lakeFS repository ID | 
**reference_id** | **str** | lakeFS reference ID (branch or commit) | 

## Example

```python
from lakefs_sdk.models.iceberg_local_table import IcebergLocalTable

# TODO update the JSON string below
json = "{}"
# create an instance of IcebergLocalTable from a JSON string
iceberg_local_table_instance = IcebergLocalTable.from_json(json)
# print the JSON string representation of the object
print IcebergLocalTable.to_json()

# convert the object into a dict
iceberg_local_table_dict = iceberg_local_table_instance.to_dict()
# create an instance of IcebergLocalTable from a dict
iceberg_local_table_form_dict = iceberg_local_table.from_dict(iceberg_local_table_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


