

# StageRangeCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**fromSourceURI** | **String** | The source location of the ingested files. Must match the lakeFS installation blockstore type. | 
**after** | **String** | Only objects after this key would be ingested. | 
**prepend** | **String** | A prefix to prepend to ingested objects. | 
**continuationToken** | **String** | Opaque. Client should pass the continuation_token received from server to continue creation ranges from the same key. |  [optional]
**stagingToken** | **String** | Opaque. Client should pass staging_token if received from server on previous request |  [optional]



