---
title: Dremio
description: This section shows how you can start using lakeFS with Dremio, a next-generation data lake engine.
---

# Using lakeFS with Dremio

[Dremio](https://www.dremio.com/) is a next-generation data lake engine that liberates your data with live, 
interactive queries directly on cloud data lake storage, including S3 and lakeFS.

## Iceberg REST Catalog

lakeFS Iceberg REST Catalog allow you to use lakeFS as a [spec-compliant](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) Apache [Iceberg REST catalog](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml), 
allowing Dremio to manage and access tables using a standard REST API.

![lakeFS Iceberg REST Catalog](../assets/img/lakefs_iceberg_rest_catalog.png)

This is the recommended way to use lakeFS with Dremio, as it allows lakeFS to stay completely outside the data path: data itself is read and written by Dremio executors, directly to the underlying object store. Metadata is managed by Iceberg at the table level, while lakeFS keeps track of new snapshots to provide versioning and isolation.

[Read more about using the Iceberg REST Catalog](./iceberg.md#iceberg-rest-catalog).    

### Configuration

To configure Dremio to work with the Iceberg REST Catalog, you need to configure the [Iceberg REST Catalog in Dremio](https://docs.dremio.com/current/data-sources/lakehouse-catalogs/iceberg-rest-catalog/).

1. On the Datasets page, to the right of **Sources** in the left panel, click `+`
1. In the **Add Data Source** dialog, under Lakehouse Catalogs, select **Iceberg REST Catalog** Source. The New Iceberg REST Catalog Source dialog box appears, which contains the following tabs:
    1. Create a name for your Iceberg REST Catalog source, specify the endpoint URI (i.e. `https://lakefs.example.com/iceberg/api`)
    1. In Advanced Options → Catalog Properties, add these (left = key, right = value):
        - `rest.auth.type` → `oauth2` 
        - `oauth2-server-uri` → your IdP’s OAuth2 token endpoint (not the catalog URL), e.g., `https://lakefs.example.com/iceberg/api/v1/oauth/tokens`
        - (optional if required by your IdP) `scope`, `audience`, or `resource`
    1. In Advanced Options → Catalog Credentials (secret properties), supply one of:
        - Client credentials flow:
            - `user` → your OAuth client_id (i.e. `${LAKEFS_ACCESS_KEY_ID}`)
            - `credential` → your client_secret (i.e. `${LAKEFS_SECRET_ACCESS_KEY}`)
    1. Click **Save** to create the Iceberg REST Catalog source.

If you're creating the data source programmatically, you can use the following JSON:

```json
{
  "config": {
    "type": "RESTCATALOG",
    "name": "iceberg_rest_oauth2",
    "host": "https://lakefs.example.com/iceberg/api",
    "propertyList": [
      {"name": "rest.auth.type", "value": "oauth2"},
      {"name": "oauth2-server-uri", "value": "https://lakefs.example.com/iceberg/api/v1/oauth/tokens"},
      {"name": "scope", "value": "catalog"}
    ],
    "secretPropertyList": [
      {"name": "user", "value": "${LAKEFS_ACCESS_KEY_ID}"},
      {"name": "credential", "value": "${LAKEFS_SECRET_ACCESS_KEY}"}
    ]
  }
}
```

!!! tip
    To learn more about the Iceberg REST Catalog, see the [Iceberg REST Catalog](./iceberg.md#iceberg-rest-catalog) documentation.

## Using Dremio with the S3 Gateway

Alternatively, you can use the S3 Gateway to read and write data to lakeFS from Dremio.

While flexible, this approach requires lakeFS to be involved in the data path, which can be less efficient than the Iceberg REST Catalog approach, since lakeFS has to proxy all data operations through the lakeFS server. This is particularly true for large data sets where network bandwidth might incur some overhead.   

### Configuration

Starting from version 3.2.3, Dremio supports Minio as an [experimental S3-compatible plugin](https://docs.dremio.com/current/sonar/data-sources/object/s3/#configuring-s3-for-minio).
Similarly, you can connect lakeFS with Dremio.

Suppose you already have both lakeFS and Dremio deployed, and want to use Dremio to query your data in the lakeFS repositories.
You can follow the steps listed below to configure on Dremio UI:

1. click _Add Data Lake_.
1. Under _File Stores_, choose _Amazon S3_.
1. Under _Advanced Options_, check _Enable compatibility mode (experimental)_.
1. Under _Advanced Options_ > _Connection Properties_, add `fs.s3a.path.style.access` and set the value to `true`.
1. Under _Advanced Options_ > _Connection Properties_, add `fs.s3a.endpoint` and set lakeFS S3 endpoint to the value. 
1. Under the _General_ tab, specify the _access_key_id_ and _secret_access_key_ provided by lakeFS server.
1. Click _Save_, and now you should be able to browse lakeFS repositories on Dremio.
