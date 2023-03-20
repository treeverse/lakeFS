# lakefs_client.model.pagination.Pagination

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**max_per_page** | decimal.Decimal, int,  | decimal.Decimal,  | Maximal number of entries per page | 
**has_more** | bool,  | BoolClass,  | Next page is available | 
**next_offset** | str,  | str,  | Token used to retrieve the next page | 
**results** | decimal.Decimal, int,  | decimal.Decimal,  | Number of values found in the results | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

