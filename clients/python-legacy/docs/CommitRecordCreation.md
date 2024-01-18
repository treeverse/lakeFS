# CommitRecordCreation


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**version** | **int** | version of the commit record | 
**commiter** | **str** | commiter of the commit record | 
**message** | **str** | message of the commit record | 
**metarange_id** | **str** | metarange_id of the commit record | 
**creation_date** | **int** | Unix Epoch in seconds | 
**parents** | **[str]** | parents of the commit record | 
**metadata** | **{str: (str,)}** | metadata of the commit record | 
**generation** | **int** | generation of the commit record | 
**force** | **bool** |  | [optional]  if omitted the server will use the default value of False
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


