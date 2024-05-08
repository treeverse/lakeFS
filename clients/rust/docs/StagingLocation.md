# StagingLocation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | Option<**String**> |  | [optional]
**presigned_url** | Option<**String**> | if presign=true is passed in the request, this field will contain a pre-signed URL to use when uploading | [optional]
**presigned_url_expiry** | Option<**i64**> | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


