

# ObjectCopyCreation


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**srcPath** | **String** | path of the copied object relative to the ref |  |
|**srcRef** | **String** | a reference, if empty uses the provided branch as ref |  [optional] |
|**force** | **Boolean** |  |  [optional] |
|**shallow** | **Boolean** | Create a shallow copy of the object (without copying the actual data). At the moment shallow copy only works for same repository and branch.  Please note that shallow copied objects might be in contention with garbage collection and branch retention policies - use with caution.  |  [optional] |



