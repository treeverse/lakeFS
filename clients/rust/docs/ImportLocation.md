# ImportLocation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **String** | Path type, can either be 'common_prefix' or 'object' | 
**path** | **String** | A source location to a 'common_prefix' or to a single object. Must match the lakeFS installation blockstore type. | 
**destination** | **String** | Destination for the imported objects on the branch. Must be a relative path to the branch. If the type is an 'object', the destination is the exact object name under the branch. If the type is a 'common_prefix', the destination is the prefix under the branch.  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


