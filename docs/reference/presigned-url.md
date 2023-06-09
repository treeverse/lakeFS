---
layout: default
title: Presigned URL
description: Configuring lakeFS to use presigned URLs
parent: Reference
nav_order: 100
has_children: false
---

# Configuring lakeFS to use presigned URLs

{% include toc_2-3.html %}

With lakeFS, you can access data directly from the storage and not through lakeFS using a presigned URL.
Based on the user's access to an object in the object store, the presigned URL will get read or write access.
The presign support is enabled for block adapter that supports it (S3, GCP, Azure), and can be disabled by the [configuration](configuration.md) (`blockstore.blockstore-name.disable_pre_signed`). Note that the UI support is disabled by default.

## Using presigned URLs in the UI
For using presigned URLs in the UI:
1. Enable the presigned URL support UI in the lakeFS [configuration](configuration.md) (`blockstore.blockstore-name.disable_pre_signed_ui`).
2. Add CORS (Cross-Origin Resource Sharing) permissions to the bucket for the UI to fetch objects using a presigned URL (instead of through lakeFS).
3. The `disable_pre_signed` needs to be enabled to enable it in the UI.

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
            "PUT"
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
        "method": ["PUT", "GET"],
        "maxAgeSeconds": 3600
    }
   ]
```


### Example: [Azure blob storage](https://learn.microsoft.com/en-us/rest/api/storageservices/cross-origin-resource-sharing--cors--support-for-the-azure-storage-services)

```xml
  <Cors>
      <CorsRule>  
          <AllowedOrigins>lakefs.endpoint</AllowedOrigins>  
          <AllowedMethods>PUT,GET</AllowedMethods>  
          <AllowedHeaders>*</AllowedHeaders>  
          <ExposedHeaders>ETag</ExposedHeaders>  
          <MaxAgeInSeconds>3600</MaxAgeInSeconds>  
      </CorsRule>  
  </Cors>
```

