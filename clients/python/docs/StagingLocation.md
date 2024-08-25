# StagingLocation

location for placing an object when staging it

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **str** |  | [optional] 
**presigned_url** | **str** | if presign&#x3D;true is passed in the request, this field will contain a pre-signed URL to use when uploading | [optional] 
**presigned_url_expiry** | **int** | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  | [optional] 

## Example

```python
from lakefs_sdk.models.staging_location import StagingLocation

# TODO update the JSON string below
json = "{}"
# create an instance of StagingLocation from a JSON string
staging_location_instance = StagingLocation.from_json(json)
# print the JSON string representation of the object
print(StagingLocation.to_json())

# convert the object into a dict
staging_location_dict = staging_location_instance.to_dict()
# create an instance of StagingLocation from a dict
staging_location_from_dict = StagingLocation.from_dict(staging_location_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


