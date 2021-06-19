# HealthCheckApi

## lakefs\_client.HealthCheckApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**health\_check**](healthcheckapi.md#health_check) | **GET** /healthcheck |  |

## **health\_check**

> health\_check\(\)

check that the API server is up and running

### Example

```python
import time
import lakefs_client
from lakefs_client.api import health_check_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = health_check_api.HealthCheckApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        api_instance.health_check()
    except lakefs_client.ApiException as e:
        print("Exception when calling HealthCheckApi->health_check: %s\n" % e)
```

### Parameters

This endpoint does not need any parameter.

### Return type

void \(empty response body\)

### Authorization

No authorization required

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**204** \| NoContent \| - \|

[\[Back to top\]](healthcheckapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

