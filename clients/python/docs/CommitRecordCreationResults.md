# CommitRecordCreationResults


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | id of the created commit record | 

## Example

```python
from lakefs_sdk.models.commit_record_creation_results import CommitRecordCreationResults

# TODO update the JSON string below
json = "{}"
# create an instance of CommitRecordCreationResults from a JSON string
commit_record_creation_results_instance = CommitRecordCreationResults.from_json(json)
# print the JSON string representation of the object
print CommitRecordCreationResults.to_json()

# convert the object into a dict
commit_record_creation_results_dict = commit_record_creation_results_instance.to_dict()
# create an instance of CommitRecordCreationResults from a dict
commit_record_creation_results_form_dict = commit_record_creation_results.from_dict(commit_record_creation_results_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


