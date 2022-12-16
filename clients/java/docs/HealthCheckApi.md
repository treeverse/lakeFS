# HealthCheckApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**healthCheck**](HealthCheckApi.md#healthCheck) | **GET** /healthcheck | 


<a name="healthCheck"></a>
# **healthCheck**
> healthCheck()



check that the API server is up and running

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.HealthCheckApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost/api/v1");

    HealthCheckApi apiInstance = new HealthCheckApi(defaultClient);
    try {
      apiInstance.healthCheck();
    } catch (ApiException e) {
      System.err.println("Exception when calling HealthCheckApi#healthCheck");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | NoContent |  -  |

