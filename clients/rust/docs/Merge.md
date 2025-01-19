# Merge

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | Option<**String**> |  | [optional]
**metadata** | Option<**std::collections::HashMap<String, String>**> |  | [optional]
**strategy** | Option<**String**> | In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch ('dest-wins') or from the source branch('source-wins'). In case no selection is made, the merge process will fail in case of a conflict | [optional]
**force** | Option<**bool**> | Allow merge into a read-only branch or into a branch with the same content | [optional][default to false]
**allow_empty** | Option<**bool**> | Allow merge when the branches have the same content | [optional][default to false]
**squash_merge** | Option<**bool**> | If set, set only the destination branch as a parent, which \"squashes\" the merge to appear as a single commit on the destination branch.  The source commit is no longer a part of the merge commit; consider adding it to the 'metadata' or 'message' fields.  This behaves like a GitHub or GitLab \"squash merge\", or in Git terms 'git merge --squash; git commit ...'.  | [optional][default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


