# lakefs_client.model.stage_range_creation.StageRangeCreation

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**fromSourceURI** | str,  | str,  | The source location of the ingested files. Must match the lakeFS installation blockstore type. | 
**prepend** | str,  | str,  | A prefix to prepend to ingested objects. | 
**after** | str,  | str,  | Only objects after this key would be ingested. | 
**continuation_token** | str,  | str,  | Opaque. Client should pass the continuation_token received from server to continue creation ranges from the same key. | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

