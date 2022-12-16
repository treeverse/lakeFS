# lakefs_client.ConfigApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_lake_fs_version**](ConfigApi.md#get_lake_fs_version) | **GET** /config/version | 
[**get_setup_state**](ConfigApi.md#get_setup_state) | **GET** /setup_lakefs | check if the lakeFS installation is already set up
[**get_storage_config**](ConfigApi.md#get_storage_config) | **GET** /config/storage | 
[**setup**](ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user
[**setup_comm_prefs**](ConfigApi.md#setup_comm_prefs) | **POST** /setup_comm_prefs | setup communications preferences


# **get_lake_fs_version**
> VersionConfig get_lake_fs_version()



get version of lakeFS server

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import config_api
from lakefs_client.model.version_config import VersionConfig
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        api_response = api_instance.get_lake_fs_version()
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ConfigApi->get_lake_fs_version: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

[**VersionConfig**](VersionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

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
import lakefs_client
from lakefs_client.api import config_api
from lakefs_client.model.error import Error
from lakefs_client.model.setup_state import SetupState
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # check if the lakeFS installation is already set up
        api_response = api_instance.get_setup_state()
        pprint(api_response)
    except lakefs_client.ApiException as e:
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
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import config_api
from lakefs_client.model.error import Error
from lakefs_client.model.storage_config import StorageConfig
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        api_response = api_instance.get_storage_config()
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ConfigApi->get_storage_config: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

[**StorageConfig**](StorageConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

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
import lakefs_client
from lakefs_client.api import config_api
from lakefs_client.model.credentials_with_secret import CredentialsWithSecret
from lakefs_client.model.setup import Setup
from lakefs_client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)
    setup = Setup(
        username="username_example",
        key=AccessKeyCredentials(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ),
    ) # Setup | 

    # example passing only required values which don't have defaults set
    try:
        # setup lakeFS and create a first user
        api_response = api_instance.setup(setup)
        pprint(api_response)
    except lakefs_client.ApiException as e:
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
**400** | bad request |  -  |
**409** | setup was already called |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setup_comm_prefs**
> NextStep setup_comm_prefs(comm_prefs_input)

setup communications preferences

### Example


```python
import time
import lakefs_client
from lakefs_client.api import config_api
from lakefs_client.model.comm_prefs_input import CommPrefsInput
from lakefs_client.model.error import Error
from lakefs_client.model.next_step import NextStep
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = lakefs_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with lakefs_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)
    comm_prefs_input = CommPrefsInput(
        email="email_example",
        feature_updates=True,
        security_updates=True,
    ) # CommPrefsInput | 

    # example passing only required values which don't have defaults set
    try:
        # setup communications preferences
        api_response = api_instance.setup_comm_prefs(comm_prefs_input)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ConfigApi->setup_comm_prefs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **comm_prefs_input** | [**CommPrefsInput**](CommPrefsInput.md)|  |

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

