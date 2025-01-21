# RepositoryCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**storage_id** | **str** | Unique identifier of the underlying data store. *EXPERIMENTAL* | [optional] 
**storage_namespace** | **str** | Filesystem URI to store the underlying data in (e.g. \&quot;s3://my-bucket/some/path/\&quot;) | 
**default_branch** | **str** |  | [optional] 
**sample_data** | **bool** |  | [optional] [default to False]
**read_only** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.repository_creation import RepositoryCreation

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryCreation from a JSON string
repository_creation_instance = RepositoryCreation.from_json(json)
# print the JSON string representation of the object
print RepositoryCreation.to_json()

# convert the object into a dict
repository_creation_dict = repository_creation_instance.to_dict()
# create an instance of RepositoryCreation from a dict
repository_creation_form_dict = repository_creation.from_dict(repository_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


