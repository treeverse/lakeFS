# lakefs_client.model.otf_diff_entry.OtfDiffEntry

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[operation_content](#operation_content)** | dict, frozendict.frozendict,  | frozendict.frozendict,  | free form content describing the returned operation diff | 
**operation_type** | str,  | str,  | the operation category (CUD) | must be one of ["create", "update", "delete", ] 
**id** | str,  | str,  |  | 
**operation** | str,  | str,  |  | 
**timestamp** | decimal.Decimal, int,  | decimal.Decimal,  |  | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# operation_content

free form content describing the returned operation diff

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | free form content describing the returned operation diff | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

