# RemoteTable


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **str** | Remote table namespace | 
**table** | **str** | Remote table name | 

## Example

```python
from lakefs_sdk.models.remote_table import RemoteTable

# TODO update the JSON string below
json = "{}"
# create an instance of RemoteTable from a JSON string
remote_table_instance = RemoteTable.from_json(json)
# print the JSON string representation of the object
print RemoteTable.to_json()

# convert the object into a dict
remote_table_dict = remote_table_instance.to_dict()
# create an instance of RemoteTable from a dict
remote_table_form_dict = remote_table.from_dict(remote_table_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


