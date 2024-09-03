# RepositoryMetadataKeys


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**keys** | **List[str]** |  | 

## Example

```python
from lakefs_sdk.models.repository_metadata_keys import RepositoryMetadataKeys

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryMetadataKeys from a JSON string
repository_metadata_keys_instance = RepositoryMetadataKeys.from_json(json)
# print the JSON string representation of the object
print(RepositoryMetadataKeys.to_json())

# convert the object into a dict
repository_metadata_keys_dict = repository_metadata_keys_instance.to_dict()
# create an instance of RepositoryMetadataKeys from a dict
repository_metadata_keys_from_dict = RepositoryMetadataKeys.from_dict(repository_metadata_keys_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


