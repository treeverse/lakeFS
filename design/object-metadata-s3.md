# Object Metadata and S3

### Overview

lakeFS keeps the following metadata per object:

1. Physical address
1. Last modified time
1. Size
1. ETag
1. Metadata
1. Address type (relative / full)

### What is missing?

Mapped two issues that we currently do not address when we work with our S3 interface:

1. Content-Type - Not keeping the posted object content-type and do not set it back when we get the object.
  [Support Content-Type](https://github.com/treeverse/lakeFS/issues/2296)
  We saw this issue when one of the clients used the value of the content type to identify a directory marker.
  Assuming there are more use-cases where the content-type is set and/or used.
1. Additional metadata - when posting an object aws enables additional metadata to set. Currently we do not process these attributes. The object level metadata we currenly have on an object can be get/set only by our open api.
  [Store some per-file metadata (rclone multipart upload fails on unexpected ETag) #2486](https://github.com/treeverse/lakeFS/issues/2486)


### Solution

AWS metadata posted with an object will be added to the object's metadata. We will map "x-amz-meta-<name>" to name/value in our metadata. The equivalent to create entry metadata using our open api.
The s3 gateway get object will map the metadata key/value back to "x-amz-meta-<name>" headers.

The content-type and the new metadata posted in multipart upload will be stored in our tracker. This information should be kept until the multipart complete is called and then the metadata is stored.

Content-type can be added in two ways:

1. Maintain a specific field for Content-Type as part of the object metadata. Will be use to set the content-type header when we return the object or HEAD information, example:

   ```json
   {
       "AcceptRanges": "bytes",
       "LastModified": "Tue, 28 Sep 2021 22:34:44 GMT",
       "ContentLength": 10485760,
       "ETag": "\"f962bf40d19fed2e80bcbaa33bd1dfe7\"",
       "ContentType": "stam/data",
       "Metadata": {}
   }
   ```

1. Use specific entry 'Content-Metadata' in the object's metadata map. The same map used in our open api. This header will have specific treatment, as we need to map it in a different way on get object. We will not enable the user to delete it and always set default value in case it is not set or set to empty string.

