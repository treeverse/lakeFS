# InternalApi

All URIs are relative to */api/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createBranchProtectionRulePreflight**](InternalApi.md#createBranchProtectionRulePreflight) | **GET** /repositories/{repository}/branch_protection/set_allowed |  |
| [**createSymlinkFile**](InternalApi.md#createSymlinkFile) | **POST** /repositories/{repository}/refs/{branch}/symlink | creates symlink files corresponding to the given directory |
| [**getAuthCapabilities**](InternalApi.md#getAuthCapabilities) | **GET** /auth/capabilities | list authentication capabilities supported |
| [**getSetupState**](InternalApi.md#getSetupState) | **GET** /setup_lakefs | check if the lakeFS installation is already set up |
| [**postStatsEvents**](InternalApi.md#postStatsEvents) | **POST** /statistics | post stats events, this endpoint is meant for internal use only |
| [**setGarbageCollectionRulesPreflight**](InternalApi.md#setGarbageCollectionRulesPreflight) | **GET** /repositories/{repository}/gc/rules/set_allowed |  |
| [**setup**](InternalApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user |
| [**setupCommPrefs**](InternalApi.md#setupCommPrefs) | **POST** /setup_comm_prefs | setup communications preferences |
| [**uploadObjectPreflight**](InternalApi.md#uploadObjectPreflight) | **GET** /repositories/{repository}/branches/{branch}/objects/stage_allowed |  |


<a id="createBranchProtectionRulePreflight"></a>
# **createBranchProtectionRulePreflight**
> createBranchProtectionRulePreflight(repository).execute();



### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

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

    InternalApi apiInstance = new InternalApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      apiInstance.createBranchProtectionRulePreflight(repository)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#createBranchProtectionRulePreflight");
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
| **204** | User has permissions to create a branch protection rule in this repository |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **409** | Resource Conflicts With Target |  -  |
| **0** | Internal Server Error |  -  |

<a id="createSymlinkFile"></a>
# **createSymlinkFile**
> StorageURI createSymlinkFile(repository, branch).location(location).execute();

creates symlink files corresponding to the given directory

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

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

    InternalApi apiInstance = new InternalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String location = "location_example"; // String | path to the table data
    try {
      StorageURI result = apiInstance.createSymlinkFile(repository, branch)
            .location(location)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#createSymlinkFile");
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
| **location** | **String**| path to the table data | [optional] |

### Return type

[**StorageURI**](StorageURI.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | location created |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **0** | Internal Server Error |  -  |

<a id="getAuthCapabilities"></a>
# **getAuthCapabilities**
> AuthCapabilities getAuthCapabilities().execute();

list authentication capabilities supported

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    InternalApi apiInstance = new InternalApi(defaultClient);
    try {
      AuthCapabilities result = apiInstance.getAuthCapabilities()
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#getAuthCapabilities");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**AuthCapabilities**](AuthCapabilities.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | auth capabilities |  -  |
| **0** | Internal Server Error |  -  |

<a id="getSetupState"></a>
# **getSetupState**
> SetupState getSetupState().execute();

check if the lakeFS installation is already set up

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    InternalApi apiInstance = new InternalApi(defaultClient);
    try {
      SetupState result = apiInstance.getSetupState()
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#getSetupState");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**SetupState**](SetupState.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | lakeFS setup state |  -  |
| **0** | Internal Server Error |  -  |

<a id="postStatsEvents"></a>
# **postStatsEvents**
> postStatsEvents(statsEventsList).execute();

post stats events, this endpoint is meant for internal use only

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

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

    InternalApi apiInstance = new InternalApi(defaultClient);
    StatsEventsList statsEventsList = new StatsEventsList(); // StatsEventsList | 
    try {
      apiInstance.postStatsEvents(statsEventsList)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#postStatsEvents");
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
| **statsEventsList** | [**StatsEventsList**](StatsEventsList.md)|  | |

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
| **204** | reported successfully |  -  |
| **400** | Bad Request |  -  |
| **401** | Unauthorized |  -  |
| **0** | Internal Server Error |  -  |

<a id="setGarbageCollectionRulesPreflight"></a>
# **setGarbageCollectionRulesPreflight**
> setGarbageCollectionRulesPreflight(repository).execute();



### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

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

    InternalApi apiInstance = new InternalApi(defaultClient);
    String repository = "repository_example"; // String | 
    try {
      apiInstance.setGarbageCollectionRulesPreflight(repository)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#setGarbageCollectionRulesPreflight");
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
| **204** | User has permissions to set garbage collection rules on this repository |  -  |
| **401** | Unauthorized |  -  |
| **404** | Resource Not Found |  -  |
| **0** | Internal Server Error |  -  |

<a id="setup"></a>
# **setup**
> CredentialsWithSecret setup(setup).execute();

setup lakeFS and create a first user

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    InternalApi apiInstance = new InternalApi(defaultClient);
    Setup setup = new Setup(); // Setup | 
    try {
      CredentialsWithSecret result = apiInstance.setup(setup)
            .execute();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#setup");
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
| **setup** | [**Setup**](Setup.md)|  | |

### Return type

[**CredentialsWithSecret**](CredentialsWithSecret.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | user created successfully |  -  |
| **400** | Bad Request |  -  |
| **409** | setup was already called |  -  |
| **0** | Internal Server Error |  -  |

<a id="setupCommPrefs"></a>
# **setupCommPrefs**
> setupCommPrefs(commPrefsInput).execute();

setup communications preferences

### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    InternalApi apiInstance = new InternalApi(defaultClient);
    CommPrefsInput commPrefsInput = new CommPrefsInput(); // CommPrefsInput | 
    try {
      apiInstance.setupCommPrefs(commPrefsInput)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#setupCommPrefs");
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
| **commPrefsInput** | [**CommPrefsInput**](CommPrefsInput.md)|  | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | communication preferences saved successfully |  -  |
| **409** | setup was already completed |  -  |
| **412** | wrong setup state for this operation |  -  |
| **0** | Internal Server Error |  -  |

<a id="uploadObjectPreflight"></a>
# **uploadObjectPreflight**
> uploadObjectPreflight(repository, branch, path).execute();



### Example
```java
// Import classes:
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.models.*;
import io.lakefs.clients.sdk.InternalApi;

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

    InternalApi apiInstance = new InternalApi(defaultClient);
    String repository = "repository_example"; // String | 
    String branch = "branch_example"; // String | 
    String path = "path_example"; // String | relative to the branch
    try {
      apiInstance.uploadObjectPreflight(repository, branch, path)
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling InternalApi#uploadObjectPreflight");
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
| **204** | User has permissions to upload this object. This does not guarantee that the upload will be successful or even possible. It indicates only the permission at the time of calling this endpoint |  -  |
| **401** | Unauthorized |  -  |
| **403** | Forbidden |  -  |
| **404** | Resource Not Found |  -  |
| **0** | Internal Server Error |  -  |

