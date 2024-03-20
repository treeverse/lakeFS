# lakefs_client.ConfigApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_config**](ConfigApi.md#get_config) | **GET** /config | 


# **get_config**
> Config get_config()



retrieve lakeFS configuration

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Basic Authentication (external_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):

```python
import time
import lakefs_client
from lakefs_client.api import config_api
from lakefs_client.model.config import Config
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

# Configure HTTP basic authorization: external_auth
configuration = lakefs_client.Configuration(
    username = 'YOUR_USERNAME',
    password = 'YOUR_PASSWORD'
)

# Configure Bearer authorization (JWT): jwt_token
configuration = lakefs_client.Configuration(
    access_token = 'YOUR_BEARER_TOKEN'
)

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        api_response = api_instance.get_config()
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ConfigApi->get_config: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

[**Config**](Config.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [external_auth](../README.md#external_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | lakeFS configuration |  -  |
**401** | Unauthorized |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

