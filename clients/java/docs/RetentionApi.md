# RetentionApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**prepareRetentionCommits**](RetentionApi.md#prepareRetentionCommits) | **POST** /repositories/{repository}/gc/prepare | save lists of active and expired commits for garbage collection


<a name="prepareRetentionCommits"></a>
# **prepareRetentionCommits**
> InlineResponse201 prepareRetentionCommits(repository)

save lists of active and expired commits for garbage collection

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.RetentionApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RetentionApi apiInstance = new RetentionApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      InlineResponse201 result = apiInstance.prepareRetentionCommits(repository);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RetentionApi#prepareRetentionCommits");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  |

### Return type

[**InlineResponse201**](InlineResponse201.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | paths to commit sets |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

