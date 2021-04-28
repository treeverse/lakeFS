# StagingApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getPhysicalAddress**](StagingApi.md#getPhysicalAddress) | **GET** /repositories/{repository}/branches/{branch}/staging/backing | get a physical address and a return token to write object to underlying storage
[**linkPhysicalAddress**](StagingApi.md#linkPhysicalAddress) | **PUT** /repositories/{repository}/branches/{branch}/staging/backing | associate staging on this physical address with a path


<a name="getPhysicalAddress"></a>
# **getPhysicalAddress**
> StagingLocation getPhysicalAddress(repository, branch, path)

get a physical address and a return token to write object to underlying storage

### Example
```java
// Import classes:
import io.treeverse.lakefs.ApiClient;
import io.treeverse.lakefs.ApiException;
import io.treeverse.lakefs.Configuration;
import io.treeverse.lakefs.auth.*;
import io.treeverse.lakefs.models.*;
import io.treeverse.lakefs.clients.StagingApi;

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

    // Configure API key authorization: jwt_token
    ApiKeyAuth jwt_token = (ApiKeyAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //jwt_token.setApiKeyPrefix("Token");

    StagingApi apiInstance = new StagingApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | 
    try {
      StagingLocation result = apiInstance.getPhysicalAddress(repository, branch, path);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling StagingApi#getPhysicalAddress");
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
 **branch** | **String**|  |
 **path** | **String**|  |

### Return type

[**StagingLocation**](StagingLocation.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | physical address for staging area |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="linkPhysicalAddress"></a>
# **linkPhysicalAddress**
> linkPhysicalAddress(repository, branch, path, stagingMetadata)

associate staging on this physical address with a path

If the supplied token matches the current staging token, associate the object as the physical address with the supplied path.  Otherwise, if staging has been committed and the token has expired, return a conflict and hint where to place the object to try again.  Caller should copy the object to the new physical address and PUT again with the new staging token.  (No need to back off, this is due to losing the race against a concurrent commit operation.) 

### Example
```java
// Import classes:
import io.treeverse.lakefs.ApiClient;
import io.treeverse.lakefs.ApiException;
import io.treeverse.lakefs.Configuration;
import io.treeverse.lakefs.auth.*;
import io.treeverse.lakefs.models.*;
import io.treeverse.lakefs.clients.StagingApi;

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

    // Configure API key authorization: jwt_token
    ApiKeyAuth jwt_token = (ApiKeyAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //jwt_token.setApiKeyPrefix("Token");

    StagingApi apiInstance = new StagingApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | 
    StagingMetadata stagingMetadata = new StagingMetadata(); // StagingMetadata | 
    try {
      apiInstance.linkPhysicalAddress(repository, branch, path, stagingMetadata);
    } catch (ApiException e) {
      System.err.println("Exception when calling StagingApi#linkPhysicalAddress");
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
 **branch** | **String**|  |
 **path** | **String**|  |
 **stagingMetadata** | [**StagingMetadata**](StagingMetadata.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successfully linked |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Internal Server Error |  -  |
**409** | conflict with a commit, try here |  -  |
**0** | Internal Server Error |  -  |

