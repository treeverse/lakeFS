# ResetCreation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | Only allowed for operation&#x3D;\&quot;staged\&quot;.  Specifies what to reset according to path.  | 
**operation** | **str** | The kind of reset operation to perform.  If \&quot;staged\&quot;, uncommitted objects according to type.  If \&quot;hard\&quot;, branch must contain no uncommitted objects, and will be reset to refer to ref.  | [optional]  if omitted the server will use the default value of "staged"
**ref** | **str** | Only allowed for operation&#x3D;\&quot;hard\&quot;.  Branch will be reset to this ref.  | [optional] 
**path** | **str** |  | [optional] 
**force** | **bool** |  | [optional]  if omitted the server will use the default value of False
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


