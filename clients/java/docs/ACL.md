

# ACL


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**permission** | **String** | Permission level to give this ACL.  \&quot;Read\&quot;, \&quot;Write\&quot;, \&quot;Super\&quot; and \&quot;Admin\&quot; are all supported.  |  |
|**allRepositories** | **Boolean** | If true, this ACL applies to all repositories, including those added in future.  Permission \&quot;Admin\&quot; allows changing ACLs, so this is necessarily true for that permission.  |  [optional] |
|**repositories** | **List&lt;String&gt;** | Apply this ACL only to these repositories. |  [optional] |



