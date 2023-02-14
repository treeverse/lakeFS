# RetentionApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**deleteGarbageCollectionRules**](RetentionApi.md#deleteGarbageCollectionRules) | **DELETE** /repositories/{repository}/gc/rules | 
[**getGarbageCollectionRules**](RetentionApi.md#getGarbageCollectionRules) | **GET** /repositories/{repository}/gc/rules | 
[**prepareGarbageCollectionCommits**](RetentionApi.md#prepareGarbageCollectionCommits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active and expired commits for garbage collection
[**prepareGarbageCollectionUncommitted**](RetentionApi.md#prepareGarbageCollectionUncommitted) | **POST** /repositories/{repository}/gc/prepare_uncommited | save repository uncommitted metadata for garbage collection
[**setGarbageCollectionRules**](RetentionApi.md#setGarbageCollectionRules) | **POST** /repositories/{repository}/gc/rules | 


<a name="deleteGarbageCollectionRules"></a>
# **deleteGarbageCollectionRules**
> deleteGarbageCollectionRules(repository)



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
      apiInstance.deleteGarbageCollectionRules(repository);
    } catch (ApiException e) {
      System.err.println("Exception when calling RetentionApi#deleteGarbageCollectionRules");
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

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | deleted garbage collection rules successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="getGarbageCollectionRules"></a>
# **getGarbageCollectionRules**
> GarbageCollectionRules getGarbageCollectionRules(repository)



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
      GarbageCollectionRules result = apiInstance.getGarbageCollectionRules(repository);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RetentionApi#getGarbageCollectionRules");
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

[**GarbageCollectionRules**](GarbageCollectionRules.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | gc rule list |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="prepareGarbageCollectionCommits"></a>
# **prepareGarbageCollectionCommits**
> GarbageCollectionPrepareResponse prepareGarbageCollectionCommits(repository, garbageCollectionPrepareRequest)

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
    GarbageCollectionPrepareRequest garbageCollectionPrepareRequest = new GarbageCollectionPrepareRequest(); // GarbageCollectionPrepareRequest | 
    try {
      GarbageCollectionPrepareResponse result = apiInstance.prepareGarbageCollectionCommits(repository, garbageCollectionPrepareRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RetentionApi#prepareGarbageCollectionCommits");
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
 **garbageCollectionPrepareRequest** | [**GarbageCollectionPrepareRequest**](GarbageCollectionPrepareRequest.md)|  | [optional]

### Return type

[**GarbageCollectionPrepareResponse**](GarbageCollectionPrepareResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | paths to commit dataset |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="prepareGarbageCollectionUncommitted"></a>
# **prepareGarbageCollectionUncommitted**
> PrepareGCUncommittedResponse prepareGarbageCollectionUncommitted(repository, prepareGCUncommittedRequest)

save repository uncommitted metadata for garbage collection

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
    PrepareGCUncommittedRequest prepareGCUncommittedRequest = new PrepareGCUncommittedRequest(); // PrepareGCUncommittedRequest | 
    try {
      PrepareGCUncommittedResponse result = apiInstance.prepareGarbageCollectionUncommitted(repository, prepareGCUncommittedRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RetentionApi#prepareGarbageCollectionUncommitted");
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
 **prepareGCUncommittedRequest** | [**PrepareGCUncommittedRequest**](PrepareGCUncommittedRequest.md)|  | [optional]

### Return type

[**PrepareGCUncommittedResponse**](PrepareGCUncommittedResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | paths to commit dataset |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="setGarbageCollectionRules"></a>
# **setGarbageCollectionRules**
> setGarbageCollectionRules(repository, garbageCollectionRules)



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
    GarbageCollectionRules garbageCollectionRules = new GarbageCollectionRules(); // GarbageCollectionRules | 
    try {
      apiInstance.setGarbageCollectionRules(repository, garbageCollectionRules);
    } catch (ApiException e) {
      System.err.println("Exception when calling RetentionApi#setGarbageCollectionRules");
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
 **garbageCollectionRules** | [**GarbageCollectionRules**](GarbageCollectionRules.md)|  |

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
**204** | set garbage collection rules successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

