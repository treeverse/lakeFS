# TagCreation

Make tag ID point at this REF.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | ID of tag to create | 
**ref** | **str** | the commit to tag | 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.tag_creation import TagCreation

# TODO update the JSON string below
json = "{}"
# create an instance of TagCreation from a JSON string
tag_creation_instance = TagCreation.from_json(json)
# print the JSON string representation of the object
print TagCreation.to_json()

# convert the object into a dict
tag_creation_dict = tag_creation_instance.to_dict()
# create an instance of TagCreation from a dict
tag_creation_form_dict = tag_creation.from_dict(tag_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


