# BranchesApi

All URIs are relative to */api/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**cherryPick**](BranchesApi.md#cherryPick) | **POST** /repositories/{repository}/branches/{branch}/cherry-pick | Replay the changes from the given commit on the branch |
| [**createBranch**](BranchesApi.md#createBranch) | **POST** /repositories/{repository}/branches | create branch |
| [**deleteBranch**](BranchesApi.md#deleteBranch) | **DELETE** /repositories/{repository}/branches/{branch} | delete branch |
| [**diffBranch**](BranchesApi.md#diffBranch) | **GET** /repositories/{repository}/branches/{branch}/diff | diff branch |
| [**getBranch**](BranchesApi.md#getBranch) | **GET** /repositories/{repository}/branches/{branch} | get branch |
| [**listBranches**](BranchesApi.md#listBranches) | **GET** /repositories/{repository}/branches | list branches |
| [**resetBranch**](BranchesApi.md#resetBranch) | **PUT** /repositories/{repository}/branches/{branch} | reset branch |
| [**revertBranch**](BranchesApi.md#revertBranch) | **POST** /repositories/{repository}/branches/{branch}/revert | revert |


<a id="cherryPick"></a>
# **cherryPick**
> Commit cherryPick(repository, branch, cherryPickCreation).execute();

Replay the changes from the given commit on the branch

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    CherryPickCreation cherryPickCreation = new CherryPickCreation(); // CherryPickCreation | 
    try {
      Commit result = apiInstance.cherryPick(repository, branch, cherryPickCreation)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#cherryPick");
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
| **branch** | **String**|  | |
| **cherryPickCreation** | [**CherryPickCreation**](CherryPickCreation.md)|  | |

### Return type

[**Commit**](Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | the cherry-pick commit |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Conflict Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="createBranch"></a>
# **createBranch**
> String createBranch(repository, branchCreation).execute();

create branch

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    BranchCreation branchCreation = new BranchCreation(); // BranchCreation | 
    try {
      String result = apiInstance.createBranch(repository, branchCreation)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#createBranch");
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
| **branchCreation** | [**BranchCreation**](BranchCreation.md)|  | |

### Return type

**String**

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/html, application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | reference |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Resource Conflicts With Target |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="deleteBranch"></a>
# **deleteBranch**
> deleteBranch(repository, branch).force(force).execute();

delete branch

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    Boolean force = false; // Boolean | 
    try {
      apiInstance.deleteBranch(repository, branch)
            .force(force)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#deleteBranch");
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
| **branch** | **String**|  | |
| **force** | **Boolean**|  | [optional] [default to false] |

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
| **204** | branch deleted successfully |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="diffBranch"></a>
# **diffBranch**
> DiffList diffBranch(repository, branch).after(after).amount(amount).prefix(prefix).delimiter(delimiter).execute();

diff branch

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String delimiter = "delimiter_example"; // String | delimiter used to group common prefixes by
    try {
      DiffList result = apiInstance.diffBranch(repository, branch)
            .after(after)
            .amount(amount)
            .prefix(prefix)
            .delimiter(delimiter)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#diffBranch");
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
| **branch** | **String**|  | |
| **after** | **String**| return items after this value | [optional] |
| **amount** | **Integer**| how many items to return | [optional] [default to 100] |
| **prefix** | **String**| return items prefixed with this value | [optional] |
| **delimiter** | **String**| delimiter used to group common prefixes by | [optional] |

### Return type

[**DiffList**](DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | diff of branch uncommitted changes |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="getBranch"></a>
# **getBranch**
> Ref getBranch(repository, branch).execute();

get branch

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    try {
      Ref result = apiInstance.getBranch(repository, branch)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#getBranch");
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
| **branch** | **String**|  | |

### Return type

[**Ref**](Ref.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | branch |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="listBranches"></a>
# **listBranches**
> RefList listBranches(repository).prefix(prefix).after(after).amount(amount).showHidden(showHidden).execute();

list branches

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    Boolean showHidden = false; // Boolean | 
    try {
      RefList result = apiInstance.listBranches(repository)
            .prefix(prefix)
            .after(after)
            .amount(amount)
            .showHidden(showHidden)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#listBranches");
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
| **prefix** | **String**| return items prefixed with this value | [optional] |
| **after** | **String**| return items after this value | [optional] |
| **amount** | **Integer**| how many items to return | [optional] [default to 100] |
| **showHidden** | **Boolean**|  | [optional] [default to false] |

### Return type

[**RefList**](RefList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | branch list |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="resetBranch"></a>
# **resetBranch**
> resetBranch(repository, branch, resetCreation).execute();

reset branch

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    ResetCreation resetCreation = new ResetCreation(); // ResetCreation | 
    try {
      apiInstance.resetBranch(repository, branch, resetCreation)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#resetBranch");
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
| **branch** | **String**|  | |
| **resetCreation** | [**ResetCreation**](ResetCreation.md)|  | |

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
| **204** | reset successful |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="revertBranch"></a>
# **revertBranch**
> revertBranch(repository, branch, revertCreation).execute();

revert

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    RevertCreation revertCreation = new RevertCreation(); // RevertCreation | 
    try {
      apiInstance.revertBranch(repository, branch, revertCreation)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling BranchesApi#revertBranch");
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
| **branch** | **String**|  | |
| **revertCreation** | [**RevertCreation**](RevertCreation.md)|  | |

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
| **204** | revert successful |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Conflict Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

