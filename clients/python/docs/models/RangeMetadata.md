# lakefs_client.model.range_metadata.RangeMetadata

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**max_key** | str,  | str,  | Last key in the range. | 
**count** | decimal.Decimal, int,  | decimal.Decimal,  | Number of records in the range. | 
**estimated_size** | decimal.Decimal, int,  | decimal.Decimal,  | Estimated size of the range in bytes | 
**id** | str,  | str,  | ID of the range. | 
**min_key** | str,  | str,  | First key in the range. | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

