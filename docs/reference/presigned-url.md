---
layout: default
title: Presigned URL
description: Configuring lakeFS to use presigned URLs.
parent: Reference
nav_order: 100
has_children: false
---

# Presigned URL
{: .no_toc }

{% include toc.html %}

With lakeFS, you can access data directly from the storage and not through lakeFS using a presined URL.
Based on the user's access to an object in the object store, the presigned URL will get read or write access.
The presign support is enabled for block adapter that supports it, and can be disabled by the [configuration](configuration.md) (blockstore.blockstore-name.disable_pre_signed). Note that the UI support is disabled by default.

# Using presigned URLs in the UI

You must enable the presigned URL support UI in the lakeFS [configuration](configuration.md) (blockstore.blockstore-name.disable_pre_signed_ui) and add CORS (Cross-Origin Resource Sharing) permissions to the bucket for the UI to fetch objects using a presigned URL (instead of through lakeFS).

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

