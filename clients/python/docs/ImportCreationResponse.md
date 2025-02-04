# ImportCreationResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the import process | 

## Example

```python
from lakefs_sdk.models.import_creation_response import ImportCreationResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ImportCreationResponse from a JSON string
import_creation_response_instance = ImportCreationResponse.from_json(json)
# print the JSON string representation of the object
print(ImportCreationResponse.to_json())

# convert the object into a dict
import_creation_response_dict = import_creation_response_instance.to_dict()
# create an instance of ImportCreationResponse from a dict
import_creation_response_from_dict = ImportCreationResponse.from_dict(import_creation_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


