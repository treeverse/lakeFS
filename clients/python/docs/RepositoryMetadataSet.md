# RepositoryMetadataSet


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metadata** | **Dict[str, str]** |  | 

## Example

```python
from lakefs_sdk.models.repository_metadata_set import RepositoryMetadataSet

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryMetadataSet from a JSON string
repository_metadata_set_instance = RepositoryMetadataSet.from_json(json)
# print the JSON string representation of the object
print(RepositoryMetadataSet.to_json())

# convert the object into a dict
repository_metadata_set_dict = repository_metadata_set_instance.to_dict()
# create an instance of RepositoryMetadataSet from a dict
repository_metadata_set_from_dict = RepositoryMetadataSet.from_dict(repository_metadata_set_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


