# ImportPagination


## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**has_more** | **bool** | More keys to be ingested. | 
**continuation_token** | **str** | Opaque. Token used to import the next range. | [optional] 
**last_key** | **str** | Last object store key that was ingested. | 
**staging_token** | **str** | Staging token for skipped objects during ingest | [optional] 

## Example

```python
from lakefs_sdk.models.import_pagination import ImportPagination

# TODO update the JSON string below
json = "{}"
# create an instance of ImportPagination from a JSON string
import_pagination_instance = ImportPagination.from_json(json)
# print the JSON string representation of the object
print ImportPagination.to_json()

# convert the object into a dict
import_pagination_dict = import_pagination_instance.to_dict()
# create an instance of ImportPagination from a dict
import_pagination_form_dict = import_pagination.from_dict(import_pagination_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


