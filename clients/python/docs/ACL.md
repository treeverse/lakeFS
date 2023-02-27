# ACL


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**permission** | **str** | Permission level to give this ACL.  \&quot;Read\&quot;, \&quot;Write\&quot;, \&quot;Super\&quot; and \&quot;Admin\&quot; are all supported.  | 
**all_repositories** | **bool** | If true, this ACL applies to all repositories, including those added in future.  Permission \&quot;Admin\&quot; allows changing ACLs, so this is necessarily true for that permission.  | [optional] 
**repositories** | **[str]** | Apply this ACL only to these repositories. | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


