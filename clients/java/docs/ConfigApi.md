# ConfigApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getGarbageCollectionConfig**](ConfigApi.md#getGarbageCollectionConfig) | **GET** /config/garbage-collection | 
[**getLakeFSVersion**](ConfigApi.md#getLakeFSVersion) | **GET** /config/version | 
[**getSetupState**](ConfigApi.md#getSetupState) | **GET** /setup_lakefs | check if the lakeFS installation is already set up
[**getStorageConfig**](ConfigApi.md#getStorageConfig) | **GET** /config/storage | 
[**setup**](ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user
[**setupCommPrefs**](ConfigApi.md#setupCommPrefs) | **POST** /setup_comm_prefs | setup communications preferences


<a name="getGarbageCollectionConfig"></a>
# **getGarbageCollectionConfig**
> GarbageCollectionConfig getGarbageCollectionConfig()



get information of gc settings

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ConfigApi;

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

    ConfigApi apiInstance = new ConfigApi(defaultClient);
    try {
      GarbageCollectionConfig result = apiInstance.getGarbageCollectionConfig();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ConfigApi#getGarbageCollectionConfig");
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

[**GarbageCollectionConfig**](GarbageCollectionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS garbage collection config |  -  |
**401** | Unauthorized |  -  |

<a name="getLakeFSVersion"></a>
# **getLakeFSVersion**
> VersionConfig getLakeFSVersion()



get version of lakeFS server

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ConfigApi;

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

    ConfigApi apiInstance = new ConfigApi(defaultClient);
    try {
      VersionConfig result = apiInstance.getLakeFSVersion();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ConfigApi#getLakeFSVersion");
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

[**VersionConfig**](VersionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS version |  -  |
**401** | Unauthorized |  -  |

<a name="getSetupState"></a>
# **getSetupState**
> SetupState getSetupState()

check if the lakeFS installation is already set up

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ConfigApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost/api/v1");

    ConfigApi apiInstance = new ConfigApi(defaultClient);
    try {
      SetupState result = apiInstance.getSetupState();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ConfigApi#getSetupState");
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
**200** | lakeFS setup state |  -  |
**0** | Internal Server Error |  -  |

<a name="getStorageConfig"></a>
# **getStorageConfig**
> StorageConfig getStorageConfig()



retrieve lakeFS storage configuration

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.auth.*;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ConfigApi;

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

    ConfigApi apiInstance = new ConfigApi(defaultClient);
    try {
      StorageConfig result = apiInstance.getStorageConfig();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ConfigApi#getStorageConfig");
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

[**StorageConfig**](StorageConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS storage configuration |  -  |
**401** | Unauthorized |  -  |

<a name="setup"></a>
# **setup**
> CredentialsWithSecret setup(setup)

setup lakeFS and create a first user

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ConfigApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost/api/v1");

    ConfigApi apiInstance = new ConfigApi(defaultClient);
    Setup setup = new Setup(); // Setup | 
    try {
      CredentialsWithSecret result = apiInstance.setup(setup);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ConfigApi#setup");
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
 **setup** | [**Setup**](Setup.md)|  |

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
**200** | user created successfully |  -  |
**400** | bad request |  -  |
**409** | setup was already called |  -  |
**0** | Internal Server Error |  -  |

<a name="setupCommPrefs"></a>
# **setupCommPrefs**
> NextStep setupCommPrefs(commPrefsInput)

setup communications preferences

### Example
```java
// Import classes:
import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.Configuration;
import io.lakefs.clients.api.models.*;
import io.lakefs.clients.api.ConfigApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost/api/v1");

    ConfigApi apiInstance = new ConfigApi(defaultClient);
    CommPrefsInput commPrefsInput = new CommPrefsInput(); // CommPrefsInput | 
    try {
      NextStep result = apiInstance.setupCommPrefs(commPrefsInput);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling ConfigApi#setupCommPrefs");
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
 **commPrefsInput** | [**CommPrefsInput**](CommPrefsInput.md)|  |

### Return type

[**NextStep**](NextStep.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | communication preferences saved successfully |  -  |
**409** | setup was already completed |  -  |
**412** | wrong setup state for this operation |  -  |
**0** | Internal Server Error |  -  |

