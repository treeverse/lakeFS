# BranchesApi

## BranchesApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**createBranch**](branchesapi.md#createBranch) | **POST** /repositories/{repository}/branches | create branch |
| [**deleteBranch**](branchesapi.md#deleteBranch) | **DELETE** /repositories/{repository}/branches/{branch} | delete branch |
| [**diffBranch**](branchesapi.md#diffBranch) | **GET** /repositories/{repository}/branches/{branch}/diff | diff branch |
| [**getBranch**](branchesapi.md#getBranch) | **GET** /repositories/{repository}/branches/{branch} | get branch |
| [**listBranches**](branchesapi.md#listBranches) | **GET** /repositories/{repository}/branches | list branches |
| [**resetBranch**](branchesapi.md#resetBranch) | **PUT** /repositories/{repository}/branches/{branch} | reset branch |
| [**revertBranch**](branchesapi.md#revertBranch) | **POST** /repositories/{repository}/branches/{branch}/revert | revert |

## **createBranch**

> String createBranch\(repository, branchCreation\)

create branch

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    BranchCreation branchCreation = new BranchCreation(); // BranchCreation | 
    try {
      String result = apiInstance.createBranch(repository, branchCreation);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **branchCreation** | [**BranchCreation**](branchcreation.md) |  |  |

### Return type

**String**

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: text/html, application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**201** \| reference \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **409** \| Resource Conflicts With Target \| - \| **0** \| Internal Server Error \| - \|

## **deleteBranch**

> deleteBranch\(repository, branch\)

delete branch

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    try {
      apiInstance.deleteBranch(repository, branch);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **branch** | **String** |  |  |

### Return type

null \(empty response body\)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**204** \| branch deleted successfully \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **diffBranch**

> DiffList diffBranch\(repository, branch, after, amount, prefix, delimiter\)

diff branch

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String delimiter = "delimiter_example"; // String | delimiter used to group common prefixes by
    try {
      DiffList result = apiInstance.diffBranch(repository, branch, after, amount, prefix, delimiter);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **branch** | **String** |  |  |
| **after** | **String** | return items after this value | \[optional\] |
| **amount** | **Integer** | how many items to return | \[optional\] \[default to 100\] |
| **prefix** | **String** | return items prefixed with this value | \[optional\] |
| **delimiter** | **String** | delimiter used to group common prefixes by | \[optional\] |

### Return type

[**DiffList**](difflist.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| diff of branch uncommitted changes \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **getBranch**

> Ref getBranch\(repository, branch\)

get branch

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    try {
      Ref result = apiInstance.getBranch(repository, branch);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **branch** | **String** |  |  |

### Return type

[**Ref**](ref.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| branch \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **listBranches**

> RefList listBranches\(repository, prefix, after, amount\)

list branches

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    try {
      RefList result = apiInstance.listBranches(repository, prefix, after, amount);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **prefix** | **String** | return items prefixed with this value | \[optional\] |
| **after** | **String** | return items after this value | \[optional\] |
| **amount** | **Integer** | how many items to return | \[optional\] \[default to 100\] |

### Return type

[**RefList**](reflist.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| branch list \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **resetBranch**

> resetBranch\(repository, branch, resetCreation\)

reset branch

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    ResetCreation resetCreation = new ResetCreation(); // ResetCreation | 
    try {
      apiInstance.resetBranch(repository, branch, resetCreation);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **branch** | **String** |  |  |
| **resetCreation** | [**ResetCreation**](resetcreation.md) |  |  |

### Return type

null \(empty response body\)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**204** \| reset successful \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **revertBranch**

> revertBranch\(repository, branch, revertCreation\)

revert

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.BranchesApi;

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

    BranchesApi apiInstance = new BranchesApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    RevertCreation revertCreation = new RevertCreation(); // RevertCreation | 
    try {
      apiInstance.revertBranch(repository, branch, revertCreation);
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

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **String** |  |  |
| **branch** | **String** |  |  |
| **revertCreation** | [**RevertCreation**](revertcreation.md) |  |  |

### Return type

null \(empty response body\)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**204** \| revert successful \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

