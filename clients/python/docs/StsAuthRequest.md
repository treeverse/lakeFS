# StsAuthRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**code** | **str** |  | 
**state** | **str** |  | 
**redirect_uri** | **str** |  | 
**ttl_seconds** | **int** | The time-to-live for the generated token in seconds.  The default value is 3600 seconds (1 hour) maximum time allowed is 12 hours.  | [optional] 

## Example

```python
from lakefs_sdk.models.sts_auth_request import StsAuthRequest

# TODO update the JSON string below
json = "{}"
# create an instance of StsAuthRequest from a JSON string
sts_auth_request_instance = StsAuthRequest.from_json(json)
# print the JSON string representation of the object
print StsAuthRequest.to_json()

# convert the object into a dict
sts_auth_request_dict = sts_auth_request_instance.to_dict()
# create an instance of StsAuthRequest from a dict
sts_auth_request_form_dict = sts_auth_request.from_dict(sts_auth_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


