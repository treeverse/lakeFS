# ExperimentalApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**otfDiff**](ExperimentalApi.md#otfDiff) | **GET** /repositories/{repository}/otf/refs/{left_ref}/diff/{right_ref} | perform otf diff


<a name="otfDiff"></a>
# **otfDiff**
> OtfDiffList otfDiff(repository, leftRef, rightRef, tablePath, type)

perform otf diff

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ExperimentalApi;

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

    // Configure API key authorization: oidc_auth
    ApiKeyAuth oidc_auth = (ApiKeyAuth) defaultClient.getAuthentication("oidc_auth");
    oidc_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //oidc_auth.setApiKeyPrefix("Token");

    // Configure API key authorization: saml_auth
    ApiKeyAuth saml_auth = (ApiKeyAuth) defaultClient.getAuthentication("saml_auth");
    saml_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //saml_auth.setApiKeyPrefix("Token");

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String leftRef = "leftRef_example"; // String | 
    String rightRef = "rightRef_example"; // String | 
    String tablePath = "tablePath_example"; // String | a path to the table location under the specified ref.
    String type = "type_example"; // String | the type of otf
    try {
      OtfDiffList result = apiInstance.otfDiff(repository, leftRef, rightRef, tablePath, type);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#otfDiff");
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
 **leftRef** | **String**|  |
 **rightRef** | **String**|  |
 **tablePath** | **String**| a path to the table location under the specified ref. |
 **type** | **String**| the type of otf |

### Return type

[**OtfDiffList**](OtfDiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | diff list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**412** | Precondition Failed |  -  |
**0** | Internal Server Error |  -  |

