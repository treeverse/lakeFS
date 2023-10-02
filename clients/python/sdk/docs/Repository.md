# Repository


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**creation_date** | **int** | Unix Epoch in seconds | 
**default_branch** | **str** |  | 
**storage_namespace** | **str** | Filesystem URI to store the underlying data in (e.g. \&quot;s3://my-bucket/some/path/\&quot;) | 

## Example

```python
from lakefs_sdk.models.repository import Repository

# TODO update the JSON string below
json = "{}"
# create an instance of Repository from a JSON string
repository_instance = Repository.from_json(json)
# print the JSON string representation of the object
print Repository.to_json()

# convert the object into a dict
repository_dict = repository_instance.to_dict()
# create an instance of Repository from a dict
repository_form_dict = repository.from_dict(repository_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


