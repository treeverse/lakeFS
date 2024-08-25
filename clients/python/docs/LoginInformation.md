# LoginInformation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_key_id** | **str** |  | 
**secret_access_key** | **str** |  | 

## Example

```python
from lakefs_sdk.models.login_information import LoginInformation

# TODO update the JSON string below
json = "{}"
# create an instance of LoginInformation from a JSON string
login_information_instance = LoginInformation.from_json(json)
# print the JSON string representation of the object
print(LoginInformation.to_json())

# convert the object into a dict
login_information_dict = login_information_instance.to_dict()
# create an instance of LoginInformation from a dict
login_information_from_dict = LoginInformation.from_dict(login_information_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


