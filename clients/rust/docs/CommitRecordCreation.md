# CommitRecordCreation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**commit_id** | **String** | id of the commit record | 
**version** | **i32** | version of the commit record | 
**committer** | **String** | committer of the commit record | 
**message** | **String** | message of the commit record | 
**metarange_id** | **String** | metarange_id of the commit record | 
**creation_date** | **i64** | Unix Epoch in seconds | 
**parents** | **Vec<String>** | parents of the commit record | 
**metadata** | Option<**std::collections::HashMap<String, String>**> | metadata of the commit record | [optional]
**generation** | **i64** | generation of the commit record | 
**force** | Option<**bool**> |  | [optional][default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


