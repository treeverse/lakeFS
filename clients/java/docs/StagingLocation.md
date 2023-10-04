

# StagingLocation

location for placing an object when staging it

## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**physicalAddress** | **String** |  |  [optional] |
|**token** | **String** | opaque staging token to use to link uploaded object |  |
|**presignedUrl** | **String** | if presign&#x3D;true is passed in the request, this field will contain a pre-signed URL to use when uploading |  [optional] |
|**presignedUrlExpiry** | **Long** | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  |  [optional] |



