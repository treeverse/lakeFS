# StagingLocation

location for placing an object when staging it

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** |  | [optional] 
**presigned_url** | **str, none_type** | if presign&#x3D;true is passed in the request, this field will contain a pre-signed URL to use when uploading | [optional] 
**presigned_url_expiry** | **int** | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


