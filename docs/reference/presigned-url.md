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

lakeFS provides the option to access the data directly from the storage and not through lakeFS using presinged URL.
lakeFS can generate a presigned URL to the object store by a user who has access to the object. The presigned URL can be used to access the object.
lakeFS assume by default the object storage support presinged URLs, if not it should be disabled in the configuration.

# Using presigned URLs in the UI

For the UI to fetch objects using a presigned URL to the object storage (instead through lakeFS), need to enable the presigned URL support UI in the lakeFS configuration and add CORS (Cross-Origin Resource Sharing) permissions to the bucket.

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
        "responseHeader": [
            "ETag"],
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

