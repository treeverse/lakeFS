# RefsApi

All URIs are relative to */api/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**diffRefs**](RefsApi.md#diffRefs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references |
| [**findMergeBase**](RefsApi.md#findMergeBase) | **GET** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | find the merge base for 2 references |
| [**logCommits**](RefsApi.md#logCommits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref. If both objects and prefixes are empty, return all commits. |
| [**mergeIntoBranch**](RefsApi.md#mergeIntoBranch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references |


<a id="diffRefs"></a>
# **diffRefs**
> DiffList diffRefs(repository, leftRef, rightRef).after(after).amount(amount).prefix(prefix).delimiter(delimiter).type(type).includeRightInfo(includeRightInfo).execute();

diff references

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String leftRef = "leftRef_example"; // String | a reference (could be either a branch or a commit ID)
    String rightRef = "rightRef_example"; // String | a reference (could be either a branch or a commit ID) to compare against
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String delimiter = "delimiter_example"; // String | delimiter used to group common prefixes by
    String type = "two_dot"; // String | 
    Boolean includeRightInfo = false; // Boolean | If set to true, the diff will include right-side object info. *EXPERIMENTAL*
    try {
      DiffList result = apiInstance.diffRefs(repository, leftRef, rightRef)
            .after(after)
            .amount(amount)
            .prefix(prefix)
            .delimiter(delimiter)
            .type(type)
            .includeRightInfo(includeRightInfo)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **leftRef** | **String**| a reference (could be either a branch or a commit ID) | |
| **rightRef** | **String**| a reference (could be either a branch or a commit ID) to compare against | |
| **after** | **String**| return items after this value | [optional] |
| **amount** | **Integer**| how many items to return | [optional] [default to 100] |
| **prefix** | **String**| return items prefixed with this value | [optional] |
| **delimiter** | **String**| delimiter used to group common prefixes by | [optional] |
| **type** | **String**|  | [optional] [default to three_dot] [enum: two_dot, three_dot] |
| **includeRightInfo** | **Boolean**| If set to true, the diff will include right-side object info. *EXPERIMENTAL* | [optional] [default to false] |

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
| **200** | diff between refs |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="findMergeBase"></a>
# **findMergeBase**
> FindMergeBaseResult findMergeBase(repository, sourceRef, destinationBranch).execute();

find the merge base for 2 references

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String sourceRef = "sourceRef_example"; // String | source ref
    String destinationBranch = "destinationBranch_example"; // String | destination branch name
    try {
      FindMergeBaseResult result = apiInstance.findMergeBase(repository, sourceRef, destinationBranch)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling RefsApi#findMergeBase");
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
| **sourceRef** | **String**| source ref | |
| **destinationBranch** | **String**| destination branch name | |

### Return type

[**FindMergeBaseResult**](FindMergeBaseResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Found the merge base |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="logCommits"></a>
# **logCommits**
> CommitList logCommits(repository, ref).after(after).amount(amount).objects(objects).prefixes(prefixes).limit(limit).firstParent(firstParent).since(since).stopAt(stopAt).execute();

get commit log from ref. If both objects and prefixes are empty, return all commits.

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String ref = "ref_example"; // String | 
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    List<String> objects = Arrays.asList(); // List<String> | list of paths, each element is a path of a specific object
    List<String> prefixes = Arrays.asList(); // List<String> | list of paths, each element is a path of a prefix
    Boolean limit = true; // Boolean | limit the number of items in return to 'amount'. Without further indication on actual number of items.
    Boolean firstParent = true; // Boolean | if set to true, follow only the first parent upon reaching a merge commit
    OffsetDateTime since = OffsetDateTime.now(); // OffsetDateTime | Show commits more recent than a specific date-time. In case used with stop_at parameter, will stop at the first commit that meets any of the conditions.
    String stopAt = "stopAt_example"; // String | A reference to stop at. In case used with since parameter, will stop at the first commit that meets any of the conditions.
    try {
      CommitList result = apiInstance.logCommits(repository, ref)
            .after(after)
            .amount(amount)
            .objects(objects)
            .prefixes(prefixes)
            .limit(limit)
            .firstParent(firstParent)
            .since(since)
            .stopAt(stopAt)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **ref** | **String**|  | |
| **after** | **String**| return items after this value | [optional] |
| **amount** | **Integer**| how many items to return | [optional] [default to 100] |
| **objects** | [**List&lt;String&gt;**](String.md)| list of paths, each element is a path of a specific object | [optional] |
| **prefixes** | [**List&lt;String&gt;**](String.md)| list of paths, each element is a path of a prefix | [optional] |
| **limit** | **Boolean**| limit the number of items in return to &#39;amount&#39;. Without further indication on actual number of items. | [optional] |
| **firstParent** | **Boolean**| if set to true, follow only the first parent upon reaching a merge commit | [optional] |
| **since** | **OffsetDateTime**| Show commits more recent than a specific date-time. In case used with stop_at parameter, will stop at the first commit that meets any of the conditions. | [optional] |
| **stopAt** | **String**| A reference to stop at. In case used with since parameter, will stop at the first commit that meets any of the conditions. | [optional] |

### Return type

[**CommitList**](CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | commit log |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="mergeIntoBranch"></a>
# **mergeIntoBranch**
> MergeResult mergeIntoBranch(repository, sourceRef, destinationBranch).merge(merge).execute();

merge references

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.RefsApi;

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

    RefsApi apiInstance = new RefsApi(defaultClient);
    String repository = "repository_example"; // String | 
    String sourceRef = "sourceRef_example"; // String | source ref
    String destinationBranch = "destinationBranch_example"; // String | destination branch name
    Merge merge = new Merge(); // Merge | 
    try {
      MergeResult result = apiInstance.mergeIntoBranch(repository, sourceRef, destinationBranch)
            .merge(merge)
            .execute();
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

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **repository** | **String**|  | |
| **sourceRef** | **String**| source ref | |
| **destinationBranch** | **String**| destination branch name | |
| **merge** | [**Merge**](Merge.md)|  | [optional] |

### Return type

[**MergeResult**](MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | merge completed |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Conflict Deprecated: content schema will return Error format and not an empty MergeResult  |  -  |
| **412** | precondition failed (e.g. a pre-merge hook returned a failure) |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

