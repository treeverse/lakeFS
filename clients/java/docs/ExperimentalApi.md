# ExperimentalApi

All URIs are relative to */api/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**abortPresignMultipartUpload**](ExperimentalApi.md#abortPresignMultipartUpload) | **DELETE** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Abort a presign multipart upload |
| [**completePresignMultipartUpload**](ExperimentalApi.md#completePresignMultipartUpload) | **PUT** /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId} | Complete a presign multipart upload request |
| [**createPresignMultipartUpload**](ExperimentalApi.md#createPresignMultipartUpload) | **POST** /repositories/{repository}/branches/{branch}/staging/pmpu | Initiate a multipart upload |
| [**createPullRequest**](ExperimentalApi.md#createPullRequest) | **POST** /repositories/{repository}/pulls | create pull request |
| [**createUserExternalPrincipal**](ExperimentalApi.md#createUserExternalPrincipal) | **POST** /auth/users/{userId}/external/principals | attach external principal to user |
| [**deleteUserExternalPrincipal**](ExperimentalApi.md#deleteUserExternalPrincipal) | **DELETE** /auth/users/{userId}/external/principals | delete external principal from user |
| [**externalPrincipalLogin**](ExperimentalApi.md#externalPrincipalLogin) | **POST** /auth/external/principal/login | perform a login using an external authenticator |
| [**getExternalPrincipal**](ExperimentalApi.md#getExternalPrincipal) | **GET** /auth/external/principals | describe external principal by id |
| [**getPullRequest**](ExperimentalApi.md#getPullRequest) | **GET** /repositories/{repository}/pulls/{pull_request} | get pull request |
| [**hardResetBranch**](ExperimentalApi.md#hardResetBranch) | **PUT** /repositories/{repository}/branches/{branch}/hard_reset | hard reset branch |
| [**listPullRequests**](ExperimentalApi.md#listPullRequests) | **GET** /repositories/{repository}/pulls | list pull requests |
| [**listUserExternalPrincipals**](ExperimentalApi.md#listUserExternalPrincipals) | **GET** /auth/users/{userId}/external/principals/ls | list user external policies attached to a user |
| [**mergePullRequest**](ExperimentalApi.md#mergePullRequest) | **PUT** /repositories/{repository}/pulls/{pull_request}/merge | merge pull request |
| [**stsLogin**](ExperimentalApi.md#stsLogin) | **POST** /sts/login | perform a login with STS |
| [**updatePullRequest**](ExperimentalApi.md#updatePullRequest) | **PATCH** /repositories/{repository}/pulls/{pull_request} | update pull request |


<a id="abortPresignMultipartUpload"></a>
# **abortPresignMultipartUpload**
> abortPresignMultipartUpload(repository, branch, uploadId, path).abortPresignMultipartUpload(abortPresignMultipartUpload).execute();

Abort a presign multipart upload

Aborts a presign multipart upload.

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String uploadId = "uploadId_example"; // String | 
    String path = "path_example"; // String | relative to the branch
    AbortPresignMultipartUpload abortPresignMultipartUpload = new AbortPresignMultipartUpload(); // AbortPresignMultipartUpload | 
    try {
      apiInstance.abortPresignMultipartUpload(repository, branch, uploadId, path)
            .abortPresignMultipartUpload(abortPresignMultipartUpload)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#abortPresignMultipartUpload");
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
| **uploadId** | **String**|  | |
| **path** | **String**| relative to the branch | |
| **abortPresignMultipartUpload** | [**AbortPresignMultipartUpload**](AbortPresignMultipartUpload.md)|  | [optional] |

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
| **204** | Presign multipart upload aborted |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="completePresignMultipartUpload"></a>
# **completePresignMultipartUpload**
> ObjectStats completePresignMultipartUpload(repository, branch, uploadId, path).completePresignMultipartUpload(completePresignMultipartUpload).execute();

Complete a presign multipart upload request

Completes a presign multipart upload by assembling the uploaded parts.

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String uploadId = "uploadId_example"; // String | 
    String path = "path_example"; // String | relative to the branch
    CompletePresignMultipartUpload completePresignMultipartUpload = new CompletePresignMultipartUpload(); // CompletePresignMultipartUpload | 
    try {
      ObjectStats result = apiInstance.completePresignMultipartUpload(repository, branch, uploadId, path)
            .completePresignMultipartUpload(completePresignMultipartUpload)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#completePresignMultipartUpload");
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
| **uploadId** | **String**|  | |
| **path** | **String**| relative to the branch | |
| **completePresignMultipartUpload** | [**CompletePresignMultipartUpload**](CompletePresignMultipartUpload.md)|  | [optional] |

### Return type

[**ObjectStats**](ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Presign multipart upload completed |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **409** | conflict with a commit, try here |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="createPresignMultipartUpload"></a>
# **createPresignMultipartUpload**
> PresignMultipartUpload createPresignMultipartUpload(repository, branch, path).parts(parts).execute();

Initiate a multipart upload

Initiates a multipart upload and returns an upload ID with presigned URLs for each part (optional). Part numbers starts with 1. Each part except the last one has minimum size depends on the underlying blockstore implementation. For example working with S3 blockstore, minimum size is 5MB (excluding the last part). 

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | relative to the branch
    Integer parts = 56; // Integer | number of presigned URL parts required to upload
    try {
      PresignMultipartUpload result = apiInstance.createPresignMultipartUpload(repository, branch, path)
            .parts(parts)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#createPresignMultipartUpload");
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
| **path** | **String**| relative to the branch | |
| **parts** | **Integer**| number of presigned URL parts required to upload | [optional] |

### Return type

[**PresignMultipartUpload**](PresignMultipartUpload.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | Presign multipart upload initiated |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="createPullRequest"></a>
# **createPullRequest**
> PullRequestCreationResponse createPullRequest(repository, pullRequestCreation).execute();

create pull request

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    PullRequestCreation pullRequestCreation = new PullRequestCreation(); // PullRequestCreation | 
    try {
      PullRequestCreationResponse result = apiInstance.createPullRequest(repository, pullRequestCreation)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#createPullRequest");
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
| **pullRequestCreation** | [**PullRequestCreation**](PullRequestCreation.md)|  | |

### Return type

[**PullRequestCreationResponse**](PullRequestCreationResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | pull request created |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Resource Conflicts With Target |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="createUserExternalPrincipal"></a>
# **createUserExternalPrincipal**
> createUserExternalPrincipal(userId, principalId).externalPrincipalCreation(externalPrincipalCreation).execute();

attach external principal to user

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String userId = "userId_example"; // String | 
    String principalId = "principalId_example"; // String | 
    ExternalPrincipalCreation externalPrincipalCreation = new ExternalPrincipalCreation(); // ExternalPrincipalCreation | 
    try {
      apiInstance.createUserExternalPrincipal(userId, principalId)
            .externalPrincipalCreation(externalPrincipalCreation)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#createUserExternalPrincipal");
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
| **userId** | **String**|  | |
| **principalId** | **String**|  | |
| **externalPrincipalCreation** | [**ExternalPrincipalCreation**](ExternalPrincipalCreation.md)|  | [optional] |

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
| **201** | external principal attached successfully |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Resource Conflicts With Target |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="deleteUserExternalPrincipal"></a>
# **deleteUserExternalPrincipal**
> deleteUserExternalPrincipal(userId, principalId).execute();

delete external principal from user

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String userId = "userId_example"; // String | 
    String principalId = "principalId_example"; // String | 
    try {
      apiInstance.deleteUserExternalPrincipal(userId, principalId)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#deleteUserExternalPrincipal");
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
| **userId** | **String**|  | |
| **principalId** | **String**|  | |

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
| **204** | external principal detached successfully |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="externalPrincipalLogin"></a>
# **externalPrincipalLogin**
> AuthenticationToken externalPrincipalLogin().externalLoginInformation(externalLoginInformation).execute();

perform a login using an external authenticator

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    ExternalLoginInformation externalLoginInformation = new ExternalLoginInformation(); // ExternalLoginInformation | 
    try {
      AuthenticationToken result = apiInstance.externalPrincipalLogin()
            .externalLoginInformation(externalLoginInformation)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#externalPrincipalLogin");
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
| **externalLoginInformation** | [**ExternalLoginInformation**](ExternalLoginInformation.md)|  | [optional] |

### Return type

[**AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful external login |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="getExternalPrincipal"></a>
# **getExternalPrincipal**
> ExternalPrincipal getExternalPrincipal(principalId).execute();

describe external principal by id

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String principalId = "principalId_example"; // String | 
    try {
      ExternalPrincipal result = apiInstance.getExternalPrincipal(principalId)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#getExternalPrincipal");
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
| **principalId** | **String**|  | |

### Return type

[**ExternalPrincipal**](ExternalPrincipal.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | external principal |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="getPullRequest"></a>
# **getPullRequest**
> PullRequest getPullRequest(repository, pullRequest).execute();

get pull request

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String pullRequest = "pullRequest_example"; // String | pull request id
    try {
      PullRequest result = apiInstance.getPullRequest(repository, pullRequest)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#getPullRequest");
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
| **pullRequest** | **String**| pull request id | |

### Return type

[**PullRequest**](PullRequest.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | pull request |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="hardResetBranch"></a>
# **hardResetBranch**
> hardResetBranch(repository, branch, ref).force(force).execute();

hard reset branch

Relocate branch to refer to ref.  Branch must not contain uncommitted data.

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String ref = "ref_example"; // String | After reset, branch will point at this reference.
    Boolean force = false; // Boolean | 
    try {
      apiInstance.hardResetBranch(repository, branch, ref)
            .force(force)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#hardResetBranch");
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
| **ref** | **String**| After reset, branch will point at this reference. | |
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
| **204** | reset successful |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="listPullRequests"></a>
# **listPullRequests**
> PullRequestsList listPullRequests(repository).prefix(prefix).after(after).amount(amount).status(status).execute();

list pull requests

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    String status = "open"; // String | 
    try {
      PullRequestsList result = apiInstance.listPullRequests(repository)
            .prefix(prefix)
            .after(after)
            .amount(amount)
            .status(status)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#listPullRequests");
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
| **status** | **String**|  | [optional] [default to all] [enum: open, closed, all] |

### Return type

[**PullRequestsList**](PullRequestsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | list of pull requests |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="listUserExternalPrincipals"></a>
# **listUserExternalPrincipals**
> ExternalPrincipalList listUserExternalPrincipals(userId).prefix(prefix).after(after).amount(amount).execute();

list user external policies attached to a user

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String userId = "userId_example"; // String | 
    String prefix = "prefix_example"; // String | return items prefixed with this value
    String after = "after_example"; // String | return items after this value
    Integer amount = 100; // Integer | how many items to return
    try {
      ExternalPrincipalList result = apiInstance.listUserExternalPrincipals(userId)
            .prefix(prefix)
            .after(after)
            .amount(amount)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#listUserExternalPrincipals");
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
| **userId** | **String**|  | |
| **prefix** | **String**| return items prefixed with this value | [optional] |
| **after** | **String**| return items after this value | [optional] |
| **amount** | **Integer**| how many items to return | [optional] [default to 100] |

### Return type

[**ExternalPrincipalList**](ExternalPrincipalList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | external principals list |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="mergePullRequest"></a>
# **mergePullRequest**
> MergeResult mergePullRequest(repository, pullRequest).execute();

merge pull request

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String pullRequest = "pullRequest_example"; // String | pull request id
    try {
      MergeResult result = apiInstance.mergePullRequest(repository, pullRequest)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#mergePullRequest");
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
| **pullRequest** | **String**| pull request id | |

### Return type

[**MergeResult**](MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
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

<a id="stsLogin"></a>
# **stsLogin**
> AuthenticationToken stsLogin(stsAuthRequest).execute();

perform a login with STS

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    StsAuthRequest stsAuthRequest = new StsAuthRequest(); // StsAuthRequest | 
    try {
      AuthenticationToken result = apiInstance.stsLogin(stsAuthRequest)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#stsLogin");
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
| **stsAuthRequest** | [**StsAuthRequest**](StsAuthRequest.md)|  | |

### Return type

[**AuthenticationToken**](AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful STS login |  -  |
| **401** | Unauthorized |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

<a id="updatePullRequest"></a>
# **updatePullRequest**
> updatePullRequest(repository, pullRequest, pullRequestBasic).execute();

update pull request

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.ExperimentalApi;

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

    ExperimentalApi apiInstance = new ExperimentalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String pullRequest = "pullRequest_example"; // String | pull request id
    PullRequestBasic pullRequestBasic = new PullRequestBasic(); // PullRequestBasic | 
    try {
      apiInstance.updatePullRequest(repository, pullRequest, pullRequestBasic)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling ExperimentalApi#updatePullRequest");
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
| **pullRequest** | **String**| pull request id | |
| **pullRequestBasic** | [**PullRequestBasic**](PullRequestBasic.md)|  | |

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
| **200** | pull request updated successfully |  -  |
| **400** | Validation Error |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **420** | too many requests |  -  |
| **0** | Internal Server Error |  -  |

