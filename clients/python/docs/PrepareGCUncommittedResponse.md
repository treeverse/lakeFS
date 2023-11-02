# PrepareGCUncommittedResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**run_id** | **str** |  | 
**gc_uncommitted_location** | **str** | location of uncommitted information data | 
**continuation_token** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.prepare_gc_uncommitted_response import PrepareGCUncommittedResponse

# TODO update the JSON string below
json = "{}"
# create an instance of PrepareGCUncommittedResponse from a JSON string
prepare_gc_uncommitted_response_instance = PrepareGCUncommittedResponse.from_json(json)
# print the JSON string representation of the object
print PrepareGCUncommittedResponse.to_json()

# convert the object into a dict
prepare_gc_uncommitted_response_dict = prepare_gc_uncommitted_response_instance.to_dict()
# create an instance of PrepareGCUncommittedResponse from a dict
prepare_gc_uncommitted_response_form_dict = prepare_gc_uncommitted_response.from_dict(prepare_gc_uncommitted_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


