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

By default, lakeFS will get and set objects using its server. lakeFS also support accessing objects using a presinged URL which allows direct access to the object.
lakeFS assume by default your object storage support presinged URLs, if not it should be disabled in the configuration.

# Using presigned URLs in the UI

For lakeFS to fetch objects through the UI using a presigned URL (instead of getting it with lakeFS) need to enable the presigned URL support UI in the lakeFS configuration and add cors permissions to the bucket.

**⚠️ Note** currently duckDB do not support presigned URL.

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

