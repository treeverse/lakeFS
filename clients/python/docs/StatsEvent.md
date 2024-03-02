# StatsEvent


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**var_class** | **str** | stats event class (e.g. \&quot;s3_gateway\&quot;, \&quot;openapi_request\&quot;, \&quot;experimental-feature\&quot;, \&quot;ui-event\&quot;) | 
**name** | **str** | stats event name (e.g. \&quot;put_object\&quot;, \&quot;create_repository\&quot;, \&quot;&lt;experimental-feature-name&gt;\&quot;) | 
**count** | **int** | number of events of the class and name | 

## Example

```python
from lakefs_sdk.models.stats_event import StatsEvent

# TODO update the JSON string below
json = "{}"
# create an instance of StatsEvent from a JSON string
stats_event_instance = StatsEvent.from_json(json)
# print the JSON string representation of the object
print StatsEvent.to_json()

# convert the object into a dict
stats_event_dict = stats_event_instance.to_dict()
# create an instance of StatsEvent from a dict
stats_event_form_dict = stats_event.from_dict(stats_event_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


