# lakefs_client.model.stats_event.StatsEvent

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**count** | decimal.Decimal, int,  | decimal.Decimal,  | number of events of the class and name | 
**name** | str,  | str,  | stats event name (e.g. \&quot;put_object\&quot;, \&quot;create_repository\&quot;, \&quot;&lt;experimental-feature-name&gt;\&quot;) | 
**class** | str,  | str,  | stats event class (e.g. \&quot;s3_gateway\&quot;, \&quot;openapi_request\&quot;, \&quot;experimental-feature\&quot;, \&quot;ui-event\&quot;) | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

