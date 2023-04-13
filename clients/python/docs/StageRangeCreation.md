# StageRangeCreation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**from_source_uri** | **str** | The source location of the ingested files. Must match the lakeFS installation blockstore type. | 
**after** | **str** | Only objects after this key would be ingested. | 
**prepend** | **str** | A prefix to prepend to ingested objects. | 
**continuation_token** | **str** | Opaque. Client should pass the continuation_token received from server to continue creation ranges from the same key. | [optional] 
**staging_token** | **str** | Opaque. Client should pass staging_token if received from server on previous request | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


