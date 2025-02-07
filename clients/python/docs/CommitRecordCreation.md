# CommitRecordCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**commit_id** | **str** | id of the commit record | 
**version** | **int** | version of the commit record | 
**committer** | **str** | committer of the commit record | 
**message** | **str** | message of the commit record | 
**metarange_id** | **str** | metarange_id of the commit record | 
**creation_date** | **int** | Unix Epoch in seconds | 
**parents** | **List[str]** | parents of the commit record | 
**metadata** | **Dict[str, str]** | metadata of the commit record | [optional] 
**generation** | **int** | generation of the commit record | 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.commit_record_creation import CommitRecordCreation

# TODO update the JSON string below
json = "{}"
# create an instance of CommitRecordCreation from a JSON string
commit_record_creation_instance = CommitRecordCreation.from_json(json)
# print the JSON string representation of the object
print CommitRecordCreation.to_json()

# convert the object into a dict
commit_record_creation_dict = commit_record_creation_instance.to_dict()
# create an instance of CommitRecordCreation from a dict
commit_record_creation_form_dict = commit_record_creation.from_dict(commit_record_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


