# CommitOverrides


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** | replace the commit message | [optional] 
**metadata** | **Dict[str, str]** | replace the metadata of the commit | [optional] 

## Example

```python
from lakefs_sdk.models.commit_overrides import CommitOverrides

# TODO update the JSON string below
json = "{}"
# create an instance of CommitOverrides from a JSON string
commit_overrides_instance = CommitOverrides.from_json(json)
# print the JSON string representation of the object
print CommitOverrides.to_json()

# convert the object into a dict
commit_overrides_dict = commit_overrides_instance.to_dict()
# create an instance of CommitOverrides from a dict
commit_overrides_form_dict = commit_overrides.from_dict(commit_overrides_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


