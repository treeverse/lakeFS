# Merge


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** |  | [optional] 
**metadata** | **Dict[str, str]** |  | [optional] 
**strategy** | **str** | In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch (&#39;dest-wins&#39;) or from the source branch(&#39;source-wins&#39;). In case no selection is made, the merge process will fail in case of a conflict | [optional] 
**force** | **bool** | Allow merge into a read-only branch or into a branch with the same content | [optional] [default to False]
**allow_empty** | **bool** | Allow merge when the branches have the same content | [optional] [default to False]
**squash_merge** | **bool** | If set, set only the destination branch as a parent, which \&quot;squashes\&quot; the merge to appear as a single commit on the destination branch.  The source commit is no longer a part of the merge commit; consider adding it to the &#39;metadata&#39; or &#39;message&#39; fields.  This behaves like a GitHub or GitLab \&quot;squash merge\&quot;, or in Git terms &#39;git merge --squash; git commit ...&#39;.  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.merge import Merge

# TODO update the JSON string below
json = "{}"
# create an instance of Merge from a JSON string
merge_instance = Merge.from_json(json)
# print the JSON string representation of the object
print Merge.to_json()

# convert the object into a dict
merge_dict = merge_instance.to_dict()
# create an instance of Merge from a dict
merge_form_dict = merge.from_dict(merge_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


