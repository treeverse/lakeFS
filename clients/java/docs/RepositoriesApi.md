# RepositoriesApi

All URIs are relative to */api/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createRepository**](RepositoriesApi.md#createRepository) | **POST** /repositories | create repository |
| [**deleteGCRules**](RepositoriesApi.md#deleteGCRules) | **DELETE** /repositories/{repository}/settings/gc_rules |  |
| [**deleteRepository**](RepositoriesApi.md#deleteRepository) | **DELETE** /repositories/{repository} | delete repository |
| [**dumpStatus**](RepositoriesApi.md#dumpStatus) | **GET** /repositories/{repository}/dump | Status of a repository dump task |
| [**dumpSubmit**](RepositoriesApi.md#dumpSubmit) | **POST** /repositories/{repository}/dump | Backup the repository metadata (tags, commits, branches) and save the backup to the object store. |
| [**getBranchProtectionRules**](RepositoriesApi.md#getBranchProtectionRules) | **GET** /repositories/{repository}/settings/branch_protection | get branch protection rules |
| [**getGCRules**](RepositoriesApi.md#getGCRules) | **GET** /repositories/{repository}/settings/gc_rules | get repository GC rules |
| [**getRepository**](RepositoriesApi.md#getRepository) | **GET** /repositories/{repository} | get repository |
| [**getRepositoryMetadata**](RepositoriesApi.md#getRepositoryMetadata) | **GET** /repositories/{repository}/metadata | get repository metadata |
| [**listRepositories**](RepositoriesApi.md#listRepositories) | **GET** /repositories | list repositories |
| [**restoreStatus**](RepositoriesApi.md#restoreStatus) | **GET** /repositories/{repository}/restore | Status of a restore request |
| [**restoreSubmit**](RepositoriesApi.md#restoreSubmit) | **POST** /repositories/{repository}/restore | Restore repository from a dump in the object store |
| [**setBranchProtectionRules**](RepositoriesApi.md#setBranchProtectionRules) | **PUT** /repositories/{repository}/settings/branch_protection |  |
| [**setGCRules**](RepositoriesApi.md#setGCRules) | **PUT** /repositories/{repository}/settings/gc_rules |  |


<a id="createRepository"></a>
# **createRepository**
> Repository createRepository(repositoryCreation).bare(bare).execute();

create repository

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    RepositoryCreation repositoryCreation = new RepositoryCreation(); // RepositoryCreation | 
    Boolean bare = false; // Boolean | If true, create a bare repository with no initial commit and branch
    try {
      Repository result = apiInstance.createRepository(repositoryCreation)
            .bare(bare)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repositoryCreation** | [**RepositoryCreation**](RepositoryCreation.md)|  | |
| **bare** | **Boolean**| If true, create a bare repository with no initial commit and branch | [optional] [default to false] |

### Return type

[**Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | repository |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **409** | Resource Conflicts With Target |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="deleteGCRules"></a>
# **deleteGCRules**
> deleteGCRules(repository).execute();



### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      apiInstance.deleteGCRules(repository)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#deleteGCRules");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | deleted garbage collection rules successfully |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="deleteRepository"></a>
# **deleteRepository**
> deleteRepository(repository).force(force).execute();

delete repository

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    Boolean force = false; // Boolean | Bypass read-only protection and delete the repository
    try {
      apiInstance.deleteRepository(repository)
            .force(force)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **force** | **Boolean**| Bypass read-only protection and delete the repository | [optional] [default to false] |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | repository deleted successfully |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="dumpStatus"></a>
# **dumpStatus**
> RepositoryDumpStatus dumpStatus(repository, taskId).execute();

Status of a repository dump task

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String taskId = "taskId_example"; // String | 
    try {
      RepositoryDumpStatus result = apiInstance.dumpStatus(repository, taskId)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#dumpStatus");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **taskId** | **String**|  | |

### Return type

[**RepositoryDumpStatus**](RepositoryDumpStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | dump task status |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="dumpSubmit"></a>
# **dumpSubmit**
> TaskInfo dumpSubmit(repository).execute();

Backup the repository metadata (tags, commits, branches) and save the backup to the object store.

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      TaskInfo result = apiInstance.dumpSubmit(repository)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#dumpSubmit");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |

### Return type

[**TaskInfo**](TaskInfo.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **202** | dump task information |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **0** | Internal Server Error |  -  |

<a id="getBranchProtectionRules"></a>
# **getBranchProtectionRules**
> List&lt;BranchProtectionRule&gt; getBranchProtectionRules(repository).execute();

get branch protection rules

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      List<BranchProtectionRule> result = apiInstance.getBranchProtectionRules(repository)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#getBranchProtectionRules");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |

### Return type

[**List&lt;BranchProtectionRule&gt;**](BranchProtectionRule.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | branch protection rules |  * ETag -  <br>  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="getGCRules"></a>
# **getGCRules**
> GarbageCollectionRules getGCRules(repository).execute();

get repository GC rules

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      GarbageCollectionRules result = apiInstance.getGCRules(repository)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#getGCRules");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |

### Return type

[**GarbageCollectionRules**](GarbageCollectionRules.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | repository GC rules |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="getRepository"></a>
# **getRepository**
> Repository getRepository(repository).execute();

get repository

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      Repository result = apiInstance.getRepository(repository)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |

### Return type

[**Repository**](Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | repository |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="getRepositoryMetadata"></a>
# **getRepositoryMetadata**
> Map&lt;String, String&gt; getRepositoryMetadata(repository).execute();

get repository metadata

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      Map<String, String> result = apiInstance.getRepositoryMetadata(repository)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#getRepositoryMetadata");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |

### Return type

**Map&lt;String, String&gt;**

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | repository metadata |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="listRepositories"></a>
# **listRepositories**
> RepositoryList listRepositories().prefix(prefix).after(after).amount(amount).search(search).execute();

list repositories

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String search = "search_example"; // String | string for searching relevant entries
    try {
      RepositoryList result = apiInstance.listRepositories()
            .prefix(prefix)
            .after(after)
            .amount(amount)
            .search(search)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **prefix** | **String**| return items prefixed with this value | [optional] |
| **after** | **String**| return items after this value | [optional] |
| **amount** | **Integer**| how many items to return | [optional] [default to 100] |
| **search** | **String**| string for searching relevant entries | [optional] |

### Return type

[**RepositoryList**](RepositoryList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | repository list |  -  |
| **401** | Unauthorized |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="restoreStatus"></a>
# **restoreStatus**
> RepositoryRestoreStatus restoreStatus(repository, taskId).execute();

Status of a restore request

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String taskId = "taskId_example"; // String | 
    try {
      RepositoryRestoreStatus result = apiInstance.restoreStatus(repository, taskId)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#restoreStatus");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **taskId** | **String**|  | |

### Return type

[**RepositoryRestoreStatus**](RepositoryRestoreStatus.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | restore task status |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="restoreSubmit"></a>
# **restoreSubmit**
> TaskInfo restoreSubmit(repository, refsRestore).execute();

Restore repository from a dump in the object store

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    RefsRestore refsRestore = new RefsRestore(); // RefsRestore | 
    try {
      TaskInfo result = apiInstance.restoreSubmit(repository, refsRestore)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#restoreSubmit");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **refsRestore** | [**RefsRestore**](RefsRestore.md)|  | |

### Return type

[**TaskInfo**](TaskInfo.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **202** | restore task created |  -  |
| **400** | Validation Error |  -  |
| **403** | Forbidden |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **0** | Internal Server Error |  -  |

<a id="setBranchProtectionRules"></a>
# **setBranchProtectionRules**
> setBranchProtectionRules(repository, branchProtectionRule).ifMatch(ifMatch).execute();



### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    List<BranchProtectionRule> branchProtectionRule = Arrays.asList(); // List<BranchProtectionRule> | 
    String ifMatch = "ifMatch_example"; // String | if provided, the branch protection rules will be updated only if the current ETag match the provided value
    try {
      apiInstance.setBranchProtectionRules(repository, branchProtectionRule)
            .ifMatch(ifMatch)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#setBranchProtectionRules");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **branchProtectionRule** | [**List&lt;BranchProtectionRule&gt;**](BranchProtectionRule.md)|  | |
| **ifMatch** | **String**| if provided, the branch protection rules will be updated only if the current ETag match the provided value | [optional] |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | branch protection rule created successfully |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **412** | Precondition Failed |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="setGCRules"></a>
# **setGCRules**
> setGCRules(repository, garbageCollectionRules).execute();



### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RepositoriesApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");
    
    // Configure HTTP basic authorization: basic_auth
    HttpBasicAuth basic_auth = (HttpBasicAuth) defaultClient.getAuthentication("basic_auth");
    basic_auth.setUsername("YOUR USERNAME");
    basic_auth.setPassword("YOUR PASSWORD");

    // Configure API key authorization: cookie_auth
    ApiKeyAuth cookie_auth = (ApiKeyAuth) defaultClient.getAuthentication("cookie_auth");
    cookie_auth.setApiKey("YOUR API KEY");
    // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
    //cookie_auth.setApiKeyPrefix("Token");

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

    // Configure HTTP bearer authorization: jwt_token
    HttpBearerAuth jwt_token = (HttpBearerAuth) defaultClient.getAuthentication("jwt_token");
    jwt_token.setBearerToken("BEARER TOKEN");

    RepositoriesApi apiInstance = new RepositoriesApi(defaultClient);
    String repository = "repository_example"; // String | 
    GarbageCollectionRules garbageCollectionRules = new GarbageCollectionRules(); // GarbageCollectionRules | 
    try {
      apiInstance.setGCRules(repository, garbageCollectionRules)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling RepositoriesApi#setGCRules");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **garbageCollectionRules** | [**GarbageCollectionRules**](GarbageCollectionRules.md)|  | |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | set garbage collection rules successfully |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

