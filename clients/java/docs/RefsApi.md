# RefsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**diffRefs**](RefsApi.md#diffRefs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
[**dumpRefs**](RefsApi.md#dumpRefs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs (tags, commits, branches) to object store
[**logCommits**](RefsApi.md#logCommits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref. If both objects and prefixes are empty, return all commits.
[**mergeIntoBranch**](RefsApi.md#mergeIntoBranch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references
[**restoreRefs**](RefsApi.md#restoreRefs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs (tags, commits, branches) from object store


<a name="diffRefs"></a>
# **diffRefs**
> DiffList diffRefs(repository, leftRef, rightRef, after, amount, prefix, delimiter, type, diffType)

diff references

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String leftRef = "leftRef_example"; // String | a reference (could be either a branch or a commit ID)
    String rightRef = "rightRef_example"; // String | a reference (could be either a branch or a commit ID) to compare against
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String delimiter = "delimiter_example"; // String | delimiter used to group common prefixes by
    String type = "type_example"; // String | 
    String diffType = "two_dot"; // String | 
    try {
      DiffList result = apiInstance.diffRefs(repository, leftRef, rightRef, after, amount, prefix, delimiter, type, diffType);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RefsApi#diffRefs");
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
 **leftRef** | **String**| a reference (could be either a branch or a commit ID) |
 **rightRef** | **String**| a reference (could be either a branch or a commit ID) to compare against |
 **after** | **String**| return items after this value | [optional]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **prefix** | **String**| return items prefixed with this value | [optional]
 **delimiter** | **String**| delimiter used to group common prefixes by | [optional]
 **type** | **String**|  | [optional]
 **diffType** | **String**|  | [optional] [default to three_dot] [enum: two_dot, three_dot]

### Return type

[**DiffList**](DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | diff between refs |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="dumpRefs"></a>
# **dumpRefs**
> RefsDump dumpRefs(repository)

Dump repository refs (tags, commits, branches) to object store

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      RefsDump result = apiInstance.dumpRefs(repository);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RefsApi#dumpRefs");
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

[**RefsDump**](RefsDump.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | refs dump |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="logCommits"></a>
# **logCommits**
> CommitList logCommits(repository, ref, after, amount, objects, prefixes, limit)

get commit log from ref. If both objects and prefixes are empty, return all commits.

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String ref = "ref_example"; // String | 
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    List<String> objects = Arrays.asList(); // List<String> | list of paths, each element is a path of a specific object
    List<String> prefixes = Arrays.asList(); // List<String> | list of paths, each element is a path of a prefix
    Boolean limit = true; // Boolean | limit the number of items in return to 'amount'. Without further indication on actual number of items.
    try {
      CommitList result = apiInstance.logCommits(repository, ref, after, amount, objects, prefixes, limit);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RefsApi#logCommits");
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
 **ref** | **String**|  |
 **after** | **String**| return items after this value | [optional]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **objects** | [**List&lt;String&gt;**](String.md)| list of paths, each element is a path of a specific object | [optional]
 **prefixes** | [**List&lt;String&gt;**](String.md)| list of paths, each element is a path of a prefix | [optional]
 **limit** | **Boolean**| limit the number of items in return to &#39;amount&#39;. Without further indication on actual number of items. | [optional]

### Return type

[**CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | commit log |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="mergeIntoBranch"></a>
# **mergeIntoBranch**
> MergeResult mergeIntoBranch(repository, sourceRef, destinationBranch, merge)

merge references

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String sourceRef = "sourceRef_example"; // String | source ref
    String destinationBranch = "destinationBranch_example"; // String | destination branch name
    Merge merge = new Merge(); // Merge | 
    try {
      MergeResult result = apiInstance.mergeIntoBranch(repository, sourceRef, destinationBranch, merge);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RefsApi#mergeIntoBranch");
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
 **sourceRef** | **String**| source ref |
 **destinationBranch** | **String**| destination branch name |
 **merge** | [**Merge**](Merge.md)|  | [optional]

### Return type

[**MergeResult**](MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | merge completed |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**409** | conflict |  -  |
**412** | precondition failed (e.g. a pre-merge hook returned a failure) |  -  |
**0** | Internal Server Error |  -  |

<a name="restoreRefs"></a>
# **restoreRefs**
> restoreRefs(repository, refsDump)

Restore repository refs (tags, commits, branches) from object store

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    RefsDump refsDump = new RefsDump(); // RefsDump | 
    try {
      apiInstance.restoreRefs(repository, refsDump);
    } catch (ApiException e) {
      System.err.println("Exception when calling RefsApi#restoreRefs");
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
 **refsDump** | [**RefsDump**](RefsDump.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | refs successfully loaded |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

