# ConfigApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
<<<<<<< HEAD
<<<<<<< HEAD
[**getStorageConfig**](ConfigApi.md#getStorageConfig) | **GET** /config/storage | 
=======
[**getStorageConfig**](ConfigApi.md#getStorageConfig) | **GET** /config | 
>>>>>>> 49847532... update swagger clients (breaking!) and fix tests
=======
[**getStorageConfig**](ConfigApi.md#getStorageConfig) | **GET** /config/storage | 
>>>>>>> e6a495e7... rename /config to /config/storage, align permission name
[**setup**](ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user


<a name="getStorageConfig"></a>
# **getStorageConfig**
<<<<<<< HEAD
<<<<<<< HEAD
> StorageConfig getStorageConfig()



retrieve lakeFS storage configuration
=======
> Config getStorageConfig()
=======
> StorageConfig getStorageConfig()
>>>>>>> e6a495e7... rename /config to /config/storage, align permission name



retrieve the lakefs storage configuration
>>>>>>> 49847532... update swagger clients (breaking!) and fix tests

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
<<<<<<< HEAD
<<<<<<< HEAD
      StorageConfig result = apiInstance.getStorageConfig();
=======
      Config result = apiInstance.getStorageConfig();
>>>>>>> 49847532... update swagger clients (breaking!) and fix tests
=======
      StorageConfig result = apiInstance.getStorageConfig();
>>>>>>> e6a495e7... rename /config to /config/storage, align permission name
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
<<<<<<< HEAD
**200** | lakeFS storage configuration |  -  |
=======
**200** | the lakefs storage configuration |  -  |
>>>>>>> 49847532... update swagger clients (breaking!) and fix tests
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

