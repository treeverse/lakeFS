

# Merge


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**message** | **String** |  |  [optional] |
|**metadata** | **Map&lt;String, String&gt;** |  |  [optional] |
|**strategy** | **String** | In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch (&#39;dest-wins&#39;) or from the source branch(&#39;source-wins&#39;). In case no selection is made, the merge process will fail in case of a conflict |  [optional] |
|**force** | **Boolean** | Allow merge into a read-only branch or into a branch with the same content |  [optional] |
|**allowEmpty** | **Boolean** | Allow merge when the branches have the same content |  [optional] |
|**squashMerge** | **Boolean** | If set, set only the destination branch as a parent, which \&quot;squashes\&quot; the merge to appear as a single commit on the destination branch.  The source commit is no longer a part of the merge commit; consider adding it to the &#39;metadata&#39; or &#39;message&#39; fields.  This behaves like a GitHub or GitLab \&quot;squash merge\&quot;, or in Git terms &#39;git merge --squash; git commit ...&#39;.  |  [optional] |



