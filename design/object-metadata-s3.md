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

The following two issues are currently not addressed by our S3 interface:

1. Content-Type - The content-type header sent part of put object and multipart upload is not kept, we expect get object to return the same content type.
   Related issues: [Support Content-Type](https://github.com/treeverse/lakeFS/issues/2296) [Support Trino AVRO format](https://github.com/treeverse/lakeFS/issues/2429)
1. User-defined metadata - when posting an object AWS enables additional metadata by passing x-amz-meta-* headers. Do do not process these headers and by passing them to our metadata on put/get, we will enable better integration S3 compatability and integration with tools like Rsync
   Related issues: [Store some per-file metadata](https://github.com/treeverse/lakeFS/issues/2486)


### Solution

The catalog entity will include ContentType as additional metadata field. The field will be added to the entntry identity calculation (unless it is empty for backward support).
On read of a previous committed entry without ContentType, default content-type will be returned.
On write a new entry we be set with the ContentType used to post the data. In case nothing is set, a default will be set on the object.
Our API (open api) will pass the content-type as additional field in any location we pass the object metadata, when we get or upload a file we use the standard content-type header to pass the entry content-type.
AWS metadata posted with an object will be added to the object's metadata. We will map "x-amz-meta-<name>" to name/value in our metadata. The equivalent to create entry metadata using our open api.
The S3 gateway get object will map the metadata key/value back to "x-amz-meta-<name>" headers.

Example of S3 head request on object with content-type will look like:

```json
{
   "AcceptRanges": "bytes",
   "LastModified": "Tue, 28 Sep 2021 22:34:44 GMT",
   "ContentLength": 10485760,
   "ETag": "\"f962bf40d19fed2e80bcbaa33bd1dfe7\"",
   "ContentType": "example/data",
   "Metadata": {
        "x-amz-meta-author": "barak"
   }
}
```

Supporting AWS user-metadata will be implemented by passing them into our entry's `Metadata` field. More information about S3 object metadata can be found [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#SysMetadata).
While using the S3 gateway,  put or multipart upload will store the `x-amz-meta-*` request headers into the object metadata. On get we will the `x-amz-meta-*` metadata keys as headers.
Our current metadata field will continue to be used. While  so no changes to the OpenAPI will be required.
