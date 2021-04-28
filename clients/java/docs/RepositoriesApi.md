# RepositoriesApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createRepository**](RepositoriesApi.md#createRepository) | **POST** /repositories | create repository
[**deleteRepository**](RepositoriesApi.md#deleteRepository) | **DELETE** /repositories/{repository} | delete repository
[**getRepository**](RepositoriesApi.md#getRepository) | **GET** /repositories/{repository} | get repository
[**listRepositories**](RepositoriesApi.md#listRepositories) | **GET** /repositories | list repositories


<a name="createRepository"></a>
# **createRepository**
> Repository createRepository(repositoryCreation, bare)

create repository

### Example
```java
// Import classes:
import io.treeverse.lakefs.ApiClient;
import io.treeverse.lakefs.ApiException;
import io.treeverse.lakefs.Configuration;
import io.treeverse.lakefs.auth.*;
import io.treeverse.lakefs.models.*;
import io.treeverse.lakefs.clients.RepositoriesApi;

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

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    RepositoryCreation repositoryCreation = new RepositoryCreation(); // RepositoryCreation | 
    Boolean bare = false; // Boolean | If true, create a bare repository with no initial commit and branch
    try {
      Repository result = apiInstance.createRepository(repositoryCreation, bare);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#createRepository");
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
 **repositoryCreation** | [**RepositoryCreation**](RepositoryCreation.md)|  |
 **bare** | **Boolean**| If true, create a bare repository with no initial commit and branch | [optional] [default to false]

### Return type

[**Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | repository |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

<a name="deleteRepository"></a>
# **deleteRepository**
> deleteRepository(repository)

delete repository

### Example
```java
// Import classes:
import io.treeverse.lakefs.ApiClient;
import io.treeverse.lakefs.ApiException;
import io.treeverse.lakefs.Configuration;
import io.treeverse.lakefs.auth.*;
import io.treeverse.lakefs.models.*;
import io.treeverse.lakefs.clients.RepositoriesApi;

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

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      apiInstance.deleteRepository(repository);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#deleteRepository");
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
**204** | repository deleted successfully |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="getRepository"></a>
# **getRepository**
> Repository getRepository(repository)

get repository

### Example
```java
// Import classes:
import io.treeverse.lakefs.ApiClient;
import io.treeverse.lakefs.ApiException;
import io.treeverse.lakefs.Configuration;
import io.treeverse.lakefs.auth.*;
import io.treeverse.lakefs.models.*;
import io.treeverse.lakefs.clients.RepositoriesApi;

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

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      Repository result = apiInstance.getRepository(repository);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#getRepository");
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

[**Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | repository |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

<a name="listRepositories"></a>
# **listRepositories**
> RepositoryList listRepositories(prefix, after, amount)

list repositories

### Example
```java
// Import classes:
import io.treeverse.lakefs.ApiClient;
import io.treeverse.lakefs.ApiException;
import io.treeverse.lakefs.Configuration;
import io.treeverse.lakefs.auth.*;
import io.treeverse.lakefs.models.*;
import io.treeverse.lakefs.clients.RepositoriesApi;

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

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    try {
      RepositoryList result = apiInstance.listRepositories(prefix, after, amount);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#listRepositories");
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
 **prefix** | **String**| return items prefixed with this value | [optional]
 **after** | **String**| return items after this value | [optional]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**RepositoryList**](RepositoryList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | repository list |  -  |
**401** | Unauthorized |  -  |
**0** | Internal Server Error |  -  |

