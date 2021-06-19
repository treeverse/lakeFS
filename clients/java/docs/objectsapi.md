# ObjectsApi

## ObjectsApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**deleteObject**](objectsapi.md#deleteObject) | **DELETE** /repositories/{repository}/branches/{branch}/objects | delete object |
| [**getObject**](objectsapi.md#getObject) | **GET** /repositories/{repository}/refs/{ref}/objects | get object content |
| [**getUnderlyingProperties**](objectsapi.md#getUnderlyingProperties) | **GET** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage |
| [**listObjects**](objectsapi.md#listObjects) | **GET** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix |
| [**stageObject**](objectsapi.md#stageObject) | **PUT** /repositories/{repository}/branches/{branch}/objects | stage an object\"s metadata for the given branch |
| [**statObject**](objectsapi.md#statObject) | **GET** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata |
| [**uploadObject**](objectsapi.md#uploadObject) | **POST** /repositories/{repository}/branches/{branch}/objects |  |

## **deleteObject**

> deleteObject\(repository, branch, path\)

delete object

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | 
    try {
      apiInstance.deleteObject(repository, branch, path);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#deleteObject");
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
| **path** | **String** |  |  |

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


**204** \| object deleted successfully \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **getObject**

> File getObject\(repository, ref, path\)

get object content

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String ref = "ref_example"; // String | a reference (could be either a branch or a commit ID)
    String path = "path_example"; // String | 
    try {
      File result = apiInstance.getObject(repository, ref, path);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#getObject");
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
| **ref** | **String** | a reference \(could be either a branch or a commit ID\) |  |
| **path** | **String** |  |  |

### Return type

[**File**](https://github.com/treeverse/lakeFS/tree/9d35f14eba038648902a59dbb091b27590525e87/clients/java/docs/File.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/octet-stream, application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| object content \|  _Content-Length -_   
 __ Last-Modified -   
  _ETag -_   
 __ Content-Disposition -   
 \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **410** \| object expired \| - \| **0** \| Internal Server Error \| - \|

## **getUnderlyingProperties**

> UnderlyingObjectProperties getUnderlyingProperties\(repository, ref, path\)

get object properties on underlying storage

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String ref = "ref_example"; // String | a reference (could be either a branch or a commit ID)
    String path = "path_example"; // String | 
    try {
      UnderlyingObjectProperties result = apiInstance.getUnderlyingProperties(repository, ref, path);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#getUnderlyingProperties");
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
| **ref** | **String** | a reference \(could be either a branch or a commit ID\) |  |
| **path** | **String** |  |  |

### Return type

[**UnderlyingObjectProperties**](underlyingobjectproperties.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| object metadata on underlying storage \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **listObjects**

> ObjectStatsList listObjects\(repository, ref, after, amount, delimiter, prefix\)

list objects under a given prefix

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String ref = "ref_example"; // String | a reference (could be either a branch or a commit ID)
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String delimiter = "delimiter_example"; // String | delimiter used to group common prefixes by
    String prefix = "prefix_example"; // String | return items prefixed with this value
    try {
      ObjectStatsList result = apiInstance.listObjects(repository, ref, after, amount, delimiter, prefix);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#listObjects");
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
| **ref** | **String** | a reference \(could be either a branch or a commit ID\) |  |
| **after** | **String** | return items after this value | \[optional\] |
| **amount** | **Integer** | how many items to return | \[optional\] \[default to 100\] |
| **delimiter** | **String** | delimiter used to group common prefixes by | \[optional\] |
| **prefix** | **String** | return items prefixed with this value | \[optional\] |

### Return type

[**ObjectStatsList**](objectstatslist.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| object listing \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **stageObject**

> ObjectStats stageObject\(repository, branch, path, objectStageCreation\)

stage an object\"s metadata for the given branch

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | 
    ObjectStageCreation objectStageCreation = new ObjectStageCreation(); // ObjectStageCreation | 
    try {
      ObjectStats result = apiInstance.stageObject(repository, branch, path, objectStageCreation);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#stageObject");
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
| **path** | **String** |  |  |
| **objectStageCreation** | [**ObjectStageCreation**](objectstagecreation.md) |  |  |

### Return type

[**ObjectStats**](objectstats.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: application/json
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**201** \| object metadata \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **0** \| Internal Server Error \| - \|

## **statObject**

> ObjectStats statObject\(repository, ref, path\)

get object metadata

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String ref = "ref_example"; // String | a reference (could be either a branch or a commit ID)
    String path = "path_example"; // String | 
    try {
      ObjectStats result = apiInstance.statObject(repository, ref, path);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#statObject");
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
| **ref** | **String** | a reference \(could be either a branch or a commit ID\) |  |
| **path** | **String** |  |  |

### Return type

[**ObjectStats**](objectstats.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| object metadata \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **410** \| object gone \(but partial metadata may be available\) \| - \| **0** \| Internal Server Error \| - \|

## **uploadObject**

> ObjectStats uploadObject\(repository, branch, path, storageClass, ifNoneMatch, content\)

### Example

```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ObjectsApi;

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

    ObjectsApi apiInstance = new ObjectsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | 
    String storageClass = "storageClass_example"; // String | 
    String ifNoneMatch = "*"; // String | Currently supports only \"*\" to allow uploading an object only if one doesn't exist yet
    File content = new File("/path/to/file"); // File | Object content to upload
    try {
      ObjectStats result = apiInstance.uploadObject(repository, branch, path, storageClass, ifNoneMatch, content);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ObjectsApi#uploadObject");
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
| **path** | **String** |  |  |
| **storageClass** | **String** |  | \[optional\] |
| **ifNoneMatch** | **String** | Currently supports only \"\*\" to allow uploading an object only if one doesn't exist yet | \[optional\] |
| **content** | **File** | Object content to upload | \[optional\] |

### Return type

[**ObjectStats**](objectstats.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: multipart/form-data
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**201** \| object metadata \| - \| **400** \| Validation Error \| - \| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| **412** \| Precondition Failed \| - \| **0** \| Internal Server Error \| - \|

