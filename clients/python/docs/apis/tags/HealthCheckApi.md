<a name="__pageTop"></a>
# lakefs_client.apis.tags.health_check_api.HealthCheckApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**health_check**](#health_check) | **get** /healthcheck | 

# **health_check**
<a name="health_check"></a>
> health_check()



check that the API server is up and running

### Example

```python
import lakefs_client
from lakefs_client.apis.tags import health_check_api
from pprint import pprint
# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "/api/v1"
)

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = health_check_api.HealthCheckApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        api_response = api_instance.health_check()
    except lakefs_client.ApiException as e:
        print("Exception when calling HealthCheckApi->health_check: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
204 | [ApiResponseFor204](#health_check.ApiResponseFor204) | NoContent

#### health_check.ApiResponseFor204
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

