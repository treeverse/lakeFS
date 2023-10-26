# ImportLocation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | Path type, can either be &#39;common_prefix&#39; or &#39;object&#39; | 
**path** | **str** | A source location to a prefix or to a single object. Must match the lakeFS installation blockstore type. | 
**destination** | **str** | Destination for the imported objects on the branch. Must be a relative path to the branch. If the type is &#39;object&#39;, the destination is the exact object name under the branch. If the type is &#39;common_prefix&#39;, the destination is the prefix under the branch.  | 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


