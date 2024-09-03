# RepositoryList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[Repository]**](Repository.md) |  | 

## Example

```python
from lakefs_sdk.models.repository_list import RepositoryList

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryList from a JSON string
repository_list_instance = RepositoryList.from_json(json)
# print the JSON string representation of the object
print(RepositoryList.to_json())

# convert the object into a dict
repository_list_dict = repository_list_instance.to_dict()
# create an instance of RepositoryList from a dict
repository_list_from_dict = RepositoryList.from_dict(repository_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


