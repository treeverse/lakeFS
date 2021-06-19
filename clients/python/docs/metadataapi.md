# MetadataApi

## lakefs\_client.MetadataApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**create\_symlink\_file**](metadataapi.md#create_symlink_file) | **POST** /repositories/{repository}/refs/{branch}/symlink | creates symlink files corresponding to the given directory |
| [**get\_meta\_range**](metadataapi.md#get_meta_range) | **GET** /repositories/{repository}/metadata/meta\_range/{meta\_range} | return URI to a meta-range file |
| [**get\_range**](metadataapi.md#get_range) | **GET** /repositories/{repository}/metadata/range/{range} | return URI to a range file |

## **create\_symlink\_file**

> StorageURI create\_symlink\_file\(repository, branch\)

creates symlink files corresponding to the given directory

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import metadata\_api

  from lakefs\_client.model.error import Error

  from lakefs\_client.model.storage\_uri import StorageURI

  from pprint import pprint

  **Defining the host is optional and defaults to** [**http://localhost/api/v1**](http://localhost/api/v1)\*\*\*\*

  **See configuration.py for a list of all supported configuration parameters.**

  configuration = lakefs\_client.Configuration\(

    host = "[http://localhost/api/v1](http://localhost/api/v1)"

  \)

## The client must configure the authentication and authorization parameters

## in accordance with the API server security policy.

## Examples for each auth method are provided below, use the example that

## satisfies your auth use case.

## Configure HTTP basic authorization: basic\_auth

configuration = lakefs\_client.Configuration\( username = 'YOUR\_USERNAME', password = 'YOUR\_PASSWORD' \)

## Configure API key authorization: cookie\_auth

configuration.api\_key\['cookie\_auth'\] = 'YOUR\_API\_KEY'

## Uncomment below to setup prefix \(e.g. Bearer\) for API key, if needed

## configuration.api\_key\_prefix\['cookie\_auth'\] = 'Bearer'

## Configure Bearer authorization \(JWT\): jwt\_token

configuration = lakefs\_client.Configuration\( access\_token = 'YOUR\_BEARER\_TOKEN' \)

## Enter a context with an instance of the API client

with lakefs\_client.ApiClient\(configuration\) as api\_client:

```text
# Create an instance of the API class
api_instance = metadata_api.MetadataApi(api_client)
repository = "repository_example" # str | 
branch = "branch_example" # str | 
location = "location_example" # str | path to the table data (optional)

# example passing only required values which don't have defaults set
try:
    # creates symlink files corresponding to the given directory
    api_response = api_instance.create_symlink_file(repository, branch)
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling MetadataApi->create_symlink_file: %s\n" % e)

# example passing only required values which don't have defaults set
# and optional values
try:
    # creates symlink files corresponding to the given directory
    api_response = api_instance.create_symlink_file(repository, branch, location=location)
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling MetadataApi->create_symlink_file: %s\n" % e)
```

```text
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **str**|  |
 **branch** | **str**|  |
 **location** | **str**| path to the table data | [optional]

### Return type

[**StorageURI**](StorageURI.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | location created |  -  |
**401** | Unauthorized |  -  |
**404** | Resource Not Found |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_meta_range**
> StorageURI get_meta_range(repository, meta_range)

return URI to a meta-range file

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Bearer (JWT) Authentication (jwt_token):
```python
import time
import lakefs_client
from lakefs_client.api import metadata_api
from lakefs_client.model.error import Error
from lakefs_client.model.storage_uri import StorageURI
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

# Enter a context with an instance of the API client
with lakefs_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = metadata_api.MetadataApi(api_client)
    repository = "repository_example" # str | 
    meta_range = "meta_range_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        # return URI to a meta-range file
        api_response = api_instance.get_meta_range(repository, meta_range)
        pprint(api_response)
    except lakefs_client.ApiException as e:
        print("Exception when calling MetadataApi->get_meta_range: %s\n" % e)
```

### Parameters

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |
| **meta\_range** | **str** |  |  |

### Return type

[**StorageURI**](storageuri.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| meta-range URI \|  _Location - redirect to S3_   
 _\| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| \*0_ \| Internal Server Error \| - \|

[\[Back to top\]](metadataapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

## **get\_range**

> StorageURI get\_range\(repository, range\)

return URI to a range file

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import metadata\_api

  from lakefs\_client.model.error import Error

  from lakefs\_client.model.storage\_uri import StorageURI

  from pprint import pprint

  **Defining the host is optional and defaults to** [**http://localhost/api/v1**](http://localhost/api/v1)\*\*\*\*

  **See configuration.py for a list of all supported configuration parameters.**

  configuration = lakefs\_client.Configuration\(

    host = "[http://localhost/api/v1](http://localhost/api/v1)"

  \)

## The client must configure the authentication and authorization parameters

## in accordance with the API server security policy.

## Examples for each auth method are provided below, use the example that

## satisfies your auth use case.

## Configure HTTP basic authorization: basic\_auth

configuration = lakefs\_client.Configuration\( username = 'YOUR\_USERNAME', password = 'YOUR\_PASSWORD' \)

## Configure API key authorization: cookie\_auth

configuration.api\_key\['cookie\_auth'\] = 'YOUR\_API\_KEY'

## Uncomment below to setup prefix \(e.g. Bearer\) for API key, if needed

## configuration.api\_key\_prefix\['cookie\_auth'\] = 'Bearer'

## Configure Bearer authorization \(JWT\): jwt\_token

configuration = lakefs\_client.Configuration\( access\_token = 'YOUR\_BEARER\_TOKEN' \)

## Enter a context with an instance of the API client

with lakefs\_client.ApiClient\(configuration\) as api\_client:

```text
# Create an instance of the API class
api_instance = metadata_api.MetadataApi(api_client)
repository = "repository_example" # str | 
range = "range_example" # str | 

# example passing only required values which don't have defaults set
try:
    # return URI to a range file
    api_response = api_instance.get_range(repository, range)
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling MetadataApi->get_range: %s\n" % e)
```

\`\`\`

### Parameters

| Name | Type | Description | Notes |
| :--- | :--- | :--- | :--- |
| **repository** | **str** |  |  |
| **range** | **str** |  |  |

### Return type

[**StorageURI**](storageuri.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| range URI \|  _Location - redirect to S3_   
 _\| **401** \| Unauthorized \| - \| **404** \| Resource Not Found \| - \| \*0_ \| Internal Server Error \| - \|

[\[Back to top\]](metadataapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

