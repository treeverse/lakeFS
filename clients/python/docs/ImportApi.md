# lakefs_client.ImportApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_meta_range**](ImportApi.md#create_meta_range) | **POST** /repositories/{repository}/branches/metaranges | create a lakeFS metarange file from the given ranges
[**ingest_range**](ImportApi.md#ingest_range) | **POST** /repositories/{repository}/branches/ranges | create a lakeFS range file from the source uri


# **create_meta_range**
> MetaRangeCreationResponse create_meta_range(repository, meta_range_creation)

create a lakeFS metarange file from the given ranges

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import import_api
from lakefs_client.model.meta_range_creation import MetaRangeCreation
from lakefs_client.model.meta_range_creation_response import MetaRangeCreationResponse
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
    api_instance = import_api.ImportApi(api_client)
    repository = "repository_example" # str | 
    meta_range_creation = MetaRangeCreation(
        ranges=[
            RangeMetadata(
                id="480e19972a6fbe98ab8e81ae5efdfd1a29037587e91244e87abd4adefffdb01c",
                min_key="production/collections/some/file_1.parquet",
                max_key="production/collections/some/file_8229.parquet",
                count=1,
                estimated_size=1,
            ),
        ],
    ) # MetaRangeCreation | 

    # example passing only required values which don't have defaults set
    try:
        # create a lakeFS metarange file from the given ranges
        api_response = api_instance.create_meta_range(repository, meta_range_creation)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ImportApi->create_meta_range: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **meta_range_creation** | [**MetaRangeCreation**](MetaRangeCreation.md)|  |

### Return type

[**MetaRangeCreationResponse**](MetaRangeCreationResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | metarange metadata |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ingest_range**
> IngestRangeCreationResponse ingest_range(repository, stage_range_creation)

create a lakeFS range file from the source uri

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
* Api Key Authentication (oidc_auth):

```python
import time
import lakefs_client
from lakefs_client.api import import_api
from lakefs_client.model.ingest_range_creation_response import IngestRangeCreationResponse
from lakefs_client.model.stage_range_creation import StageRangeCreation
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
    api_instance = import_api.ImportApi(api_client)
    repository = "repository_example" # str | 
    stage_range_creation = StageRangeCreation(
        from_source_uri="s3://my-bucket/production/collections/",
        after="production/collections/some/file.parquet",
        prepend="collections/",
        continuation_token="continuation_token_example",
    ) # StageRangeCreation | 

    # example passing only required values which don't have defaults set
    try:
        # create a lakeFS range file from the source uri
        api_response = api_instance.ingest_range(repository, stage_range_creation)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling ImportApi->ingest_range: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **stage_range_creation** | [**StageRangeCreation**](StageRangeCreation.md)|  |

### Return type

[**IngestRangeCreationResponse**](IngestRangeCreationResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token), [oidc_auth](../README.md#oidc_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | range metadata |  -  |
**400** | Validation Error |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

