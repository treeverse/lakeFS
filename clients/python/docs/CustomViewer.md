# CustomViewer


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**url** | **str** |  | 
**extensions** | **List[str]** |  | [optional] 
**content_types** | **List[str]** |  | [optional] 

## Example

```python
from lakefs_sdk.models.custom_viewer import CustomViewer

# TODO update the JSON string below
json = "{}"
# create an instance of CustomViewer from a JSON string
custom_viewer_instance = CustomViewer.from_json(json)
# print the JSON string representation of the object
print CustomViewer.to_json()

# convert the object into a dict
custom_viewer_dict = custom_viewer_instance.to_dict()
# create an instance of CustomViewer from a dict
custom_viewer_form_dict = custom_viewer.from_dict(custom_viewer_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


