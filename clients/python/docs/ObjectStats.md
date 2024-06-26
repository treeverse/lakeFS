# ObjectStats


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**path** | **str** |  | 
**path_type** | **str** |  | 
**physical_address** | **str** | The location of the object on the underlying object store. Formatted as a native URI with the object store type as scheme (\&quot;s3://...\&quot;, \&quot;gs://...\&quot;, etc.) Or, in the case of presign&#x3D;true, will be an HTTP URL to be consumed via regular HTTP GET  | 
**physical_address_expiry** | **int** | If present and nonzero, physical_address is a pre-signed URL and will expire at this Unix Epoch time.  This will be shorter than the pre-signed URL lifetime if an authentication token is about to expire.  This field is *optional*.  | [optional] 
**checksum** | **str** |  | 
**size_bytes** | **int** | The number of bytes in the object.  lakeFS always populates this field when returning ObjectStats.  This field is optional _for the client_ to supply, for instance on upload.  | [optional] 
**mtime** | **int** | Unix Epoch in seconds | 
**metadata** | **Dict[str, str]** |  | [optional] 
**content_type** | **str** | Object media type | [optional] 

## Example

```python
from lakefs_sdk.models.object_stats import ObjectStats

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectStats from a JSON string
object_stats_instance = ObjectStats.from_json(json)
# print the JSON string representation of the object
print ObjectStats.to_json()

# convert the object into a dict
object_stats_dict = object_stats_instance.to_dict()
# create an instance of ObjectStats from a dict
object_stats_form_dict = object_stats.from_dict(object_stats_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


