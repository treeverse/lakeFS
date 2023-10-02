# lakefs_sdk.ConfigApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_garbage_collection_config**](ConfigApi.md#get_garbage_collection_config) | **GET** /config/garbage-collection | 
[**get_lake_fs_version**](ConfigApi.md#get_lake_fs_version) | **GET** /config/version | 
[**get_setup_state**](ConfigApi.md#get_setup_state) | **GET** /setup_lakefs | check if the lakeFS installation is already set up
[**get_storage_config**](ConfigApi.md#get_storage_config) | **GET** /config/storage | 
[**setup**](ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user
[**setup_comm_prefs**](ConfigApi.md#setup_comm_prefs) | **POST** /setup_comm_prefs | setup communications preferences


# **get_garbage_collection_config**
> GarbageCollectionConfig get_garbage_collection_config()



get information of gc settings

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.garbage_collection_config import GarbageCollectionConfig
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.ConfigApi(api_client)

    try:
        api_response = api_instance.get_garbage_collection_config()
        print("The response of ConfigApi->get_garbage_collection_config:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigApi->get_garbage_collection_config: %s\n" % e)
```



### Parameters
This endpoint does not need any parameter.

### Return type

[**GarbageCollectionConfig**](GarbageCollectionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS garbage collection config |  -  |
**401** | Unauthorized |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lake_fs_version**
> VersionConfig get_lake_fs_version()



get version of lakeFS server

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.version_config import VersionConfig
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.ConfigApi(api_client)

    try:
        api_response = api_instance.get_lake_fs_version()
        print("The response of ConfigApi->get_lake_fs_version:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigApi->get_lake_fs_version: %s\n" % e)
```



### Parameters
This endpoint does not need any parameter.

### Return type

[**VersionConfig**](VersionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS version |  -  |
**401** | Unauthorized |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_setup_state**
> SetupState get_setup_state()

check if the lakeFS installation is already set up

### Example

```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.setup_state import SetupState
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.ConfigApi(api_client)

    try:
        # check if the lakeFS installation is already set up
        api_response = api_instance.get_setup_state()
        print("The response of ConfigApi->get_setup_state:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigApi->get_setup_state: %s\n" % e)
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_storage_config**
> StorageConfig get_storage_config()



retrieve lakeFS storage configuration

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.storage_config import StorageConfig
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.ConfigApi(api_client)

    try:
        api_response = api_instance.get_storage_config()
        print("The response of ConfigApi->get_storage_config:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigApi->get_storage_config: %s\n" % e)
```



### Parameters
This endpoint does not need any parameter.

### Return type

[**StorageConfig**](StorageConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS storage configuration |  -  |
**401** | Unauthorized |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setup**
> CredentialsWithSecret setup(setup)

setup lakeFS and create a first user

### Example

```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.credentials_with_secret import CredentialsWithSecret
from lakefs_sdk.models.setup import Setup
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.ConfigApi(api_client)
    setup = lakefs_sdk.Setup() # Setup | 

    try:
        # setup lakeFS and create a first user
        api_response = api_instance.setup(setup)
        print("The response of ConfigApi->setup:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigApi->setup: %s\n" % e)
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
**400** | Bad Request |  -  |
**409** | setup was already called |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setup_comm_prefs**
> setup_comm_prefs(comm_prefs_input)

setup communications preferences

### Example

```python
import time
import os
import lakefs_sdk
from lakefs_sdk.models.comm_prefs_input import CommPrefsInput
from lakefs_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_sdk.Configuration(
    host = "/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = lakefs_sdk.ConfigApi(api_client)
    comm_prefs_input = lakefs_sdk.CommPrefsInput() # CommPrefsInput | 

    try:
        # setup communications preferences
        api_instance.setup_comm_prefs(comm_prefs_input)
    except Exception as e:
        print("Exception when calling ConfigApi->setup_comm_prefs: %s\n" % e)
```



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **comm_prefs_input** | [**CommPrefsInput**](CommPrefsInput.md)|  | 

### Return type

void (empty response body)

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

