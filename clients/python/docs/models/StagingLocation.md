# lakefs_client.model.staging_location.StagingLocation

location for placing an object when staging it

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | location for placing an object when staging it | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**token** | str,  | str,  | opaque staging token to use to link uploaded object | 
**physical_address** | str,  | str,  |  | [optional] 
**presigned_url** | None, str,  | NoneClass, str,  | if presign&#x3D;true is passed in the request, this field will contain a presigned URL to use when uploading | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

