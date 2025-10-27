# User

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** | A unique identifier for the user. Cannot be edited. | 
**creation_date** | **i64** | Unix Epoch in seconds | 
**friendly_name** | Option<**String**> | A shorter name for the user than the id. Unlike id it does not identify the user (it might not be unique). Used in some places in the UI.  | [optional]
**email** | Option<**String**> | The email address of the user. If API authentication is enabled, this field is mandatory and will be invited to login. If API authentication is disabled, this field will be ignored. All current APIAuthenticators require the email to be  lowercase and unique, although custom authenticators may not enforce this.  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


