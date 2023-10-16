

# User


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**id** | **String** | A unique identifier for the user. Cannot be edited. |  |
|**creationDate** | **Long** | Unix Epoch in seconds |  |
|**friendlyName** | **String** | A shorter name for the user than the id. Unlike id it does not identify the user (it might not be unique). Used in some places in the UI.  |  [optional] |
|**email** | **String** | The email address of the user. If API authentication is enabled, this field is mandatory and will be invited to login. If API authentication is disabled, this field will be ignored. All current APIAuthenticators require the email to be  lowercase and unique, although custom authenticators may not enforce this.  |  [optional] |



