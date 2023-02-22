# ACL


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**permission** | **str** | Permission level to give this ACL.  \&quot;Read\&quot;, \&quot;Write\&quot;, \&quot;Super\&quot; and \&quot;Admin\&quot; are all supported.  | 
**repositories** | **[str]** | Apply this ACL only to these repositories.  If missing then the ACL applies to all repositories.  Admins can directly change permissions so irrelevant restriction to them.  | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


