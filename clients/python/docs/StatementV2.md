# StatementV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**effect** | **str** |  | 
**resource** | **List[str]** |  | 
**action** | **List[str]** |  | 

## Example

```python
from lakefs_sdk.models.statement_v2 import StatementV2

# TODO update the JSON string below
json = "{}"
# create an instance of StatementV2 from a JSON string
statement_v2_instance = StatementV2.from_json(json)
# print the JSON string representation of the object
print StatementV2.to_json()

# convert the object into a dict
statement_v2_dict = statement_v2_instance.to_dict()
# create an instance of StatementV2 from a dict
statement_v2_form_dict = statement_v2.from_dict(statement_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


