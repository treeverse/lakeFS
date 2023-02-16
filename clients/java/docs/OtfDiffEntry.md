

# OtfDiffEntry


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** |  |  [optional]
**timestamp** | **Integer** |  | 
**operation** | **String** |  | 
**operationContent** | **Object** | free form content describing the returned operation diff | 
**operationType** | [**OperationTypeEnum**](#OperationTypeEnum) | the operation category (CUD) | 



## Enum: OperationTypeEnum

Name | Value
---- | -----
CREATE | &quot;create&quot;
UPDATE | &quot;update&quot;
DELETE | &quot;delete&quot;



