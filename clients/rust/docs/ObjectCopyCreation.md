# ObjectCopyCreation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**src_path** | **String** | path of the copied object relative to the ref | 
**src_ref** | Option<**String**> | a reference, if empty uses the provided branch as ref | [optional]
**force** | Option<**bool**> |  | [optional][default to false]
**shallow** | Option<**bool**> | Create a shallow copy of the object (without copying the actual data). At the moment shallow copy only works for same repository and branch.  Please note that shallow copied objects might be in contention with garbage collection and branch retention policies - use with caution.  | [optional][default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


