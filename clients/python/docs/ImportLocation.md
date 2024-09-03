# ImportLocation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | Path type, can either be &#39;common_prefix&#39; or &#39;object&#39; | 
**path** | **str** | A source location to a &#39;common_prefix&#39; or to a single object. Must match the lakeFS installation blockstore type. | 
**destination** | **str** | Destination for the imported objects on the branch. Must be a relative path to the branch. If the type is an &#39;object&#39;, the destination is the exact object name under the branch. If the type is a &#39;common_prefix&#39;, the destination is the prefix under the branch.  | 

## Example

```python
from lakefs_sdk.models.import_location import ImportLocation

# TODO update the JSON string below
json = "{}"
# create an instance of ImportLocation from a JSON string
import_location_instance = ImportLocation.from_json(json)
# print the JSON string representation of the object
print(ImportLocation.to_json())

# convert the object into a dict
import_location_dict = import_location_instance.to_dict()
# create an instance of ImportLocation from a dict
import_location_from_dict = ImportLocation.from_dict(import_location_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


