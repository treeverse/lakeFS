

# ResetCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**operation** | [**OperationEnum**](#OperationEnum) | The kind of reset operation to perform.  If \&quot;staged\&quot;, uncommitted objects according to type.  If \&quot;hard\&quot;, branch must contain no uncommitted objects, and will be reset to refer to ref.  |  [optional]
**type** | [**TypeEnum**](#TypeEnum) | Only allowed for operation&#x3D;\&quot;staged\&quot;.  Specifies what to reset according to path.  | 
**ref** | **String** | Only allowed for operation&#x3D;\&quot;hard\&quot;.  Branch will be reset to this ref.  |  [optional]
**path** | **String** |  |  [optional]
**force** | **Boolean** |  |  [optional]



## Enum: OperationEnum

Name | Value
---- | -----
STAGED | &quot;staged&quot;
HARD | &quot;hard&quot;



## Enum: TypeEnum

Name | Value
---- | -----
OBJECT | &quot;object&quot;
COMMON_PREFIX | &quot;common_prefix&quot;
RESET | &quot;reset&quot;



