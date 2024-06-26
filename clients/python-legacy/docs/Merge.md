# Merge


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** |  | [optional] 
**metadata** | **{str: (str,)}** |  | [optional] 
**strategy** | **str** | In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch (&#39;dest-wins&#39;) or from the source branch(&#39;source-wins&#39;). In case no selection is made, the merge process will fail in case of a conflict | [optional] 
**force** | **bool** | Allow merge into a read-only branch or into a branch with the same content | [optional]  if omitted the server will use the default value of False
**allow_empty** | **bool** | Allow merge when the branches have the same content | [optional]  if omitted the server will use the default value of False
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


