---
title: Presigned URLs
description: Configuring lakeFS to use presigned URLs
parent: Security
redirect_from:
  - /reference/presigned-url.html
---

# Configuring lakeFS to use presigned URLs

{% include toc_2-3.html %}

With lakeFS, you can access data directly from the storage and not through lakeFS using a presigned URL.
Based on the user's access to an object in the object store, the presigned URL will get read or write access.
The presign support is enabled for block adapter that supports it (AWS, GCP, Azure), and can be disabled by the [configuration]({% link reference/configuration.md %}) (`blockstore.<blockstore_type>.disable_pre_signed`). Note that the UI support is disabled by default.

- It is possible to override the default pre-signed URL endpoint in **AWS** by setting the [configuration]({% link reference/configuration.md %}) (`blockstore.s3.pre_signed_endpoint`).
This is useful, for example, when you wish to define a [VPC endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html#accessing-s3-interface-endpoints) access for the pre-signed URL.

## Using presigned URLs in the UI
For using presigned URLs in the UI:
1. Enable the presigned URL support UI in the lakeFS [configuration]({% link reference/configuration.md %}) (`blockstore.<blockstore_type>.disable_pre_signed_ui`   ).
2. Add CORS (Cross-Origin Resource Sharing) permissions to the bucket for the UI to fetch objects using a presigned URL (instead of through lakeFS).
3. The `blockstore.<blockstore_type>.disable_pre_signed` must be false to enable it in the UI.

**⚠️ Note** Currently DuckDB fetching data from lakeFS does not support fetching data using presigned URL.

### Example: [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enabling-cors-examples.html)

```json
  [
    {
        "AllowedHeaders": [
            "*"
        ],
        "AllowedMethods": [
            "GET",
            "PUT",
            "HEAD"
        ],
        "AllowedOrigins": [
            "lakefs.endpoint"
        ],
        "ExposeHeaders": [
            "ETag"
        ]
    }
  ]
```


### Example: [Google Storage](https://cloud.google.com/storage/docs/using-cors)

```json
  [
    {
        "origin": ["lakefs.endpoint"],
        "responseHeader": ["ETag"],
        "method": ["PUT", "GET", "HEAD"],
        "maxAgeSeconds": 3600
    }
   ]
```


### Example: [Azure blob storage](https://learn.microsoft.com/en-us/rest/api/storageservices/cross-origin-resource-sharing--cors--support-for-the-azure-storage-services)

```xml
  <Cors>
      <CorsRule>  
          <AllowedOrigins>lakefs.endpoint</AllowedOrigins>  
          <AllowedMethods>PUT,GET,HEAD</AllowedMethods>  
          <AllowedHeaders>*</AllowedHeaders>  
          <ExposedHeaders>ETag,x-ms-*</ExposedHeaders>  
          <MaxAgeInSeconds>3600</MaxAgeInSeconds>  
      </CorsRule>  
  </Cors>
```

