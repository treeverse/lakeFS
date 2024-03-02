# PrepareGCUncommittedRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**continuation_token** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.prepare_gc_uncommitted_request import PrepareGCUncommittedRequest

# TODO update the JSON string below
json = "{}"
# create an instance of PrepareGCUncommittedRequest from a JSON string
prepare_gc_uncommitted_request_instance = PrepareGCUncommittedRequest.from_json(json)
# print the JSON string representation of the object
print PrepareGCUncommittedRequest.to_json()

# convert the object into a dict
prepare_gc_uncommitted_request_dict = prepare_gc_uncommitted_request_instance.to_dict()
# create an instance of PrepareGCUncommittedRequest from a dict
prepare_gc_uncommitted_request_form_dict = prepare_gc_uncommitted_request.from_dict(prepare_gc_uncommitted_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


