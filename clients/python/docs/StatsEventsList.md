# StatsEventsList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**events** | [**List[StatsEvent]**](StatsEvent.md) |  | 

## Example

```python
from lakefs_sdk.models.stats_events_list import StatsEventsList

# TODO update the JSON string below
json = "{}"
# create an instance of StatsEventsList from a JSON string
stats_events_list_instance = StatsEventsList.from_json(json)
# print the JSON string representation of the object
print StatsEventsList.to_json()

# convert the object into a dict
stats_events_list_dict = stats_events_list_instance.to_dict()
# create an instance of StatsEventsList from a dict
stats_events_list_form_dict = stats_events_list.from_dict(stats_events_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


