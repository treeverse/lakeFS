# StorageURI

URI to a path in a storage provider (e.g. \"s3://bucket1/path/to/object\")

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**location** | **str** |  | 

## Example

```python
from lakefs_sdk.models.storage_uri import StorageURI

# TODO update the JSON string below
json = "{}"
# create an instance of StorageURI from a JSON string
storage_uri_instance = StorageURI.from_json(json)
# print the JSON string representation of the object
print(StorageURI.to_json())

# convert the object into a dict
storage_uri_dict = storage_uri_instance.to_dict()
# create an instance of StorageURI from a dict
storage_uri_from_dict = StorageURI.from_dict(storage_uri_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


