# AccessKeyCredentials


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_key_id** | **str** | access key ID to set for user for use in integration testing. | 
**secret_access_key** | **str** | secret access key to set for user for use in integration testing. | 

## Example

```python
from lakefs_sdk.models.access_key_credentials import AccessKeyCredentials

# TODO update the JSON string below
json = "{}"
# create an instance of AccessKeyCredentials from a JSON string
access_key_credentials_instance = AccessKeyCredentials.from_json(json)
# print the JSON string representation of the object
print(AccessKeyCredentials.to_json())

# convert the object into a dict
access_key_credentials_dict = access_key_credentials_instance.to_dict()
# create an instance of AccessKeyCredentials from a dict
access_key_credentials_from_dict = AccessKeyCredentials.from_dict(access_key_credentials_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


