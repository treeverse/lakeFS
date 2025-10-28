# IcebergPushRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source** | [**LocalTable**](LocalTable.md) |  | 
**destination** | [**RemoteTable**](RemoteTable.md) |  | 
**force_update** | **bool** | Override exiting table in remote if exists | [optional] [default to False]
**create_namespace** | **bool** | Creates namespace in remote catalog if not exist | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.iceberg_push_request import IcebergPushRequest

# TODO update the JSON string below
json = "{}"
# create an instance of IcebergPushRequest from a JSON string
iceberg_push_request_instance = IcebergPushRequest.from_json(json)
# print the JSON string representation of the object
print IcebergPushRequest.to_json()

# convert the object into a dict
iceberg_push_request_dict = iceberg_push_request_instance.to_dict()
# create an instance of IcebergPushRequest from a dict
iceberg_push_request_form_dict = iceberg_push_request.from_dict(iceberg_push_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


