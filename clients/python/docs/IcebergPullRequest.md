# IcebergPullRequest


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source** | [**IcebergRemoteTable**](IcebergRemoteTable.md) |  | 
**destination** | [**IcebergLocalTable**](IcebergLocalTable.md) |  | 
**force_update** | **bool** | Override exiting local table if exists | [optional] [default to False]
**create_namespace** | **bool** | Creates namespace in local catalog if not exist | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.iceberg_pull_request import IcebergPullRequest

# TODO update the JSON string below
json = "{}"
# create an instance of IcebergPullRequest from a JSON string
iceberg_pull_request_instance = IcebergPullRequest.from_json(json)
# print the JSON string representation of the object
print IcebergPullRequest.to_json()

# convert the object into a dict
iceberg_pull_request_dict = iceberg_pull_request_instance.to_dict()
# create an instance of IcebergPullRequest from a dict
iceberg_pull_request_form_dict = iceberg_pull_request.from_dict(iceberg_pull_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


