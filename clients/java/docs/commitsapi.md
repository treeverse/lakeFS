# CommitsApi

## CommitsApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**commit**](commitsapi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit |
| [**getCommit**](commitsapi.md#getCommit) | **GET** /repositories/{repository}/commits/{commitId} | get commit |
| [**logBranchCommits**](commitsapi.md#logBranchCommits) | **GET** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref |

## **commit**

> Commit commit\(repository, branch, commitCreation\)

create commit

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.CommitsApi;

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

    CommitsApi apiInstance = new CommitsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    CommitCreation commitCreation = new CommitCreation(); // CommitCreation | 
    try {
      Commit result = apiInstance.commit(repository, branch, commitCreation);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling CommitsApi#commit");
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
| **commitCreation** | [**CommitCreation**](commitcreation.md) |  |  |

### Return type

[**Commit**](commit.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**201** \| commit \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **412** \| Precondition Failed \(e.g. a pre-commit hook returned a failure\) \| - \| **0** \| Internal Server Error \| - \|

## **getCommit**

> Commit getCommit\(repository, commitId\)

get commit

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.CommitsApi;

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

    CommitsApi apiInstance = new CommitsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String commitId = "commitId_example"; // String | 
    try {
      Commit result = apiInstance.getCommit(repository, commitId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling CommitsApi#getCommit");
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
| **commitId** | **String** |  |  |

### Return type

[**Commit**](commit.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| commit \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **logBranchCommits**

> CommitList logBranchCommits\(repository, branch, after, amount\)

get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.CommitsApi;

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

    CommitsApi apiInstance = new CommitsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    try {
      CommitList result = apiInstance.logBranchCommits(repository, branch, after, amount);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling CommitsApi#logBranchCommits");
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

### Return type

[**CommitList**](commitlist.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| commit log \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

