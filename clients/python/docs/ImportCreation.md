# ImportCreation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**paths** | [**List[ImportLocation]**](ImportLocation.md) |  | 
**commit** | [**CommitCreation**](CommitCreation.md) |  | 
**force** | **bool** |  | [optional] [default to False]

## Example

```python
from lakefs_sdk.models.import_creation import ImportCreation

# TODO update the JSON string below
json = "{}"
# create an instance of ImportCreation from a JSON string
import_creation_instance = ImportCreation.from_json(json)
# print the JSON string representation of the object
print ImportCreation.to_json()

# convert the object into a dict
import_creation_dict = import_creation_instance.to_dict()
# create an instance of ImportCreation from a dict
import_creation_form_dict = import_creation.from_dict(import_creation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


