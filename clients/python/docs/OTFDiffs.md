# OTFDiffs


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**diffs** | [**List[DiffProperties]**](DiffProperties.md) |  | [optional] 

## Example

```python
from lakefs_sdk.models.otf_diffs import OTFDiffs

# TODO update the JSON string below
json = "{}"
# create an instance of OTFDiffs from a JSON string
otf_diffs_instance = OTFDiffs.from_json(json)
# print the JSON string representation of the object
print OTFDiffs.to_json()

# convert the object into a dict
otf_diffs_dict = otf_diffs_instance.to_dict()
# create an instance of OTFDiffs from a dict
otf_diffs_form_dict = otf_diffs.from_dict(otf_diffs_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


