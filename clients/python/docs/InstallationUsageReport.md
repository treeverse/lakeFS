# InstallationUsageReport


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**installation_id** | **str** |  | 
**reports** | [**List[UsageReport]**](UsageReport.md) |  | 

## Example

```python
from lakefs_sdk.models.installation_usage_report import InstallationUsageReport

# TODO update the JSON string below
json = "{}"
# create an instance of InstallationUsageReport from a JSON string
installation_usage_report_instance = InstallationUsageReport.from_json(json)
# print the JSON string representation of the object
print(InstallationUsageReport.to_json())

# convert the object into a dict
installation_usage_report_dict = installation_usage_report_instance.to_dict()
# create an instance of InstallationUsageReport from a dict
installation_usage_report_from_dict = InstallationUsageReport.from_dict(installation_usage_report_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


