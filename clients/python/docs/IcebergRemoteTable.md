# IcebergRemoteTable


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** | Reference to one or more levels of a namespace | 
**table** | **str** | Remote table name | 

## Example

```python
from lakefs_sdk.models.iceberg_remote_table import IcebergRemoteTable

# TODO update the JSON string below
json = "{}"
# create an instance of IcebergRemoteTable from a JSON string
iceberg_remote_table_instance = IcebergRemoteTable.from_json(json)
# print the JSON string representation of the object
print IcebergRemoteTable.to_json()

# convert the object into a dict
iceberg_remote_table_dict = iceberg_remote_table_instance.to_dict()
# create an instance of IcebergRemoteTable from a dict
iceberg_remote_table_form_dict = iceberg_remote_table.from_dict(iceberg_remote_table_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


