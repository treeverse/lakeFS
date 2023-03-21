# lakefs_client.model.action_run.ActionRun

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**start_time** | str, datetime,  | str,  |  | value must conform to RFC-3339 date-time
**event_type** | str,  | str,  |  | 
**run_id** | str,  | str,  |  | 
**branch** | str,  | str,  |  | 
**commit_id** | str,  | str,  |  | 
**status** | str,  | str,  |  | must be one of ["failed", "completed", ] 
**end_time** | str, datetime,  | str,  |  | [optional] value must conform to RFC-3339 date-time
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

