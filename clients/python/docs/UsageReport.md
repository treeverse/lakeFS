# UsageReport


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**year** | **int** |  | 
**month** | **int** |  | 
**count** | **int** |  | 

## Example

```python
from lakefs_sdk.models.usage_report import UsageReport

# TODO update the JSON string below
json = "{}"
# create an instance of UsageReport from a JSON string
usage_report_instance = UsageReport.from_json(json)
# print the JSON string representation of the object
print(UsageReport.to_json())

# convert the object into a dict
usage_report_dict = usage_report_instance.to_dict()
# create an instance of UsageReport from a dict
usage_report_from_dict = UsageReport.from_dict(usage_report_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


