# lakeFS Iceberg REST Catalog Router

## Goals

1. Allow existing Iceberg REST Catalog implementations to integrate with lakeFS

## Non-Goals

1. We are not creating our own REST Catalog implementation, but simply route requests to an existing catalog, modifying it as needed.
2. In the future, we might want to consider implementing (or deploying an existing) Iceberg catalog in our cloud solution and provide a managed REST catalog solution
    that works with lakeFS.

## lakeFS - Iceberg REST Catalog Terminology

warehouse <--> lakeFS repository URI
prefix <--> catalog name (`lakefs`)
namespace <--> in the context of a catalog (prefix) contains a set of namespaces / table descriptors. In lakeFS catalog, namespace is prefixed with the branch name.

## IcebergRouter Service

A new service in lakeFS which will serve as the Iceberg REST catalog endpoint, intercept requests - modify them if needed and re-route them to the
actual Iceberg catalog endpoint which will be provided in the lakeFS Server configuration by the user.
This service will implement the [REST catalog open API spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
For the most part the router will simply pass the request to the Iceberg catalog as is. For some APIs, we require some pre-processing and modifications to the request
before passing it to the catalog implementation.

### Service Endpoint

lakeFS will expose a new endpoint `/iceberg`, which will be used to serve the REST Catalog interface by the new IcebergRouter service.
Example url with local lakeFS server: `https://localhost:8000/iceberg/api/v1/{prefix}/namespaces`


### What changes are required

The basic API endpoint requires providing either `prefix`, `namespace` or both. In lakeFS context, this - as well as the warehouse parameter, require different processing than usual.
The Iceberg REST catalog API should know how to handle requests that address the `lakefs` catalog (prefix) as well as request to other catalogs, therefore our router should distinguish 
between the different request paths and handle them appropriately.

Below is the distinction between the 2 types of requests based on the provided `prefix`

1. Requests with the `lakefs` prefix
   1. These requests are to the `lakefs` versioned catalog and should be parsed and pre-processed.
   2.  Catalog properties: The `warehouse` configuration parameter should reflect the `lakeFS repository` in the format of a lakeFS URI, i.e. `lakefs://{repo_name}`.
   3. The first `namespace` level will reflect the `lakeFS branch` on which to perform the API request.
2. Other requests (without the `lakefs` prefix) should be passed as is. 

## REST Catalog Implementation Requirements

The underlying REST Catalog will require the ability to interpret the lakeFS URI. This can be done by integrating the lakeFS Hadoop FileSystem (lakeFSFS) into the catalog.
In addition, we will need create a package similar to LakeFSFileIO which will enable writing the location metadata in a relative format in order to avoid referencing the lakeFS branch as
part of the location.

### Modifying the request

In the relevant APIs, perform the following modifications to the request, before passing it along:
For the following request parameters:
warehouse = `lakefs://example-repo`
prefix = `lakefs`
namespace (or table name) = `dev.accounting.tax.paid.info`

1. Override the catalog `warehouse` configuration, append the branch name to the lakeFS URI (e.g. `lakefs://example-repo/dev`)
2. Remove branch prefix from request's `namespace` parameter (e.g. `dev.accounting.tax.paid.info` ==> `accounting.tax.paid.info`)

These changes will ensure that:

1. Table namespaces are decoupled from the lakeFS branch
2. Table location is written with consideration of the lakeFS branch:  
"location": "lakefs://example-repo/dev/db/table1"

>**Note:** The above tries to be in alignment with the current lakeFS Iceberg [implementation](https://docs.lakefs.io/integrations/iceberg.html#using-iceberg-tables-with-lakefs)

## New lakeFS Server Configuration

New configuration for the Iceberg router service allows providing the REST catalog endpoint which the requests will be redirected to

```golang
IcebergRouter struct {
    Endpoint       *string `mapstructure:"endpoint"`
} `mapstructure:"iceberg_router"`

```