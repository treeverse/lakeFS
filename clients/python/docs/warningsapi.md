# WarningsApi

## lakefs\_client.WarningsApi

All URIs are relative to [http://localhost/api/v1](http://localhost/api/v1)

| Method | HTTP request | Description |
| :--- | :--- | :--- |
| [**get\_warnings**](warningsapi.md#get_warnings) | **GET** /warnings |  |

## **get\_warnings**

> Warnings get\_warnings\(\)

return all current \"global\" warnings to show user

### Example

* Basic Authentication \(basic\_auth\):
* Api Key Authentication \(cookie\_auth\):
* Bearer \(JWT\) Authentication \(jwt\_token\):

  \`\`\`python

  import time

  import lakefs\_client

  from lakefs\_client.api import warnings\_api

  from lakefs\_client.model.error import Error

  from lakefs\_client.model.warnings import Warnings

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
api_instance = warnings_api.WarningsApi(api_client)

# example, this endpoint has no required or optional parameters
try:
    api_response = api_instance.get_warnings()
    pprint(api_response)
except lakefs_client.ApiException as e:
    print("Exception when calling WarningsApi->get_warnings: %s\n" % e)
```

\`\`\`

### Parameters

This endpoint does not need any parameter.

### Return type

[**Warnings**](warnings.md)

### Authorization

[basic\_auth](../#basic_auth), [cookie\_auth](../#cookie_auth), [jwt\_token](../#jwt_token)

### HTTP request headers

* **Content-Type**: Not defined
* **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
| :--- | :--- | :--- |


**200** \| current warnings \| - \| **401** \| Unauthorized \| - \|

[\[Back to top\]](warningsapi.md) [\[Back to API list\]](../#documentation-for-api-endpoints) [\[Back to Model list\]](../#documentation-for-models) [\[Back to README\]](../)

