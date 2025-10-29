# OpenAPI for S3 Like Multipart Upload Proposal using Presigned URLs

## Introduction

This proposal outlines the development of Open API endpoints to enable multipart upload capabilities similar to Amazon S3 multipart using presigned URLs to upload parts. The intention is to enhance the data uploading process for large files, ensuring efficient and reliable data transfer.

## Goal

- Implementing multi-part upload APIs with support for parallel upload optimizes data transfer for large files over varying network conditions using lakectl.

## Non-goals

- Support all underlying storage types as we can experiment with the one that currently support this feature.
- SDK to wrap the new API and support this capability.


## Features and Functionalities

To minimize the scope of this feature and understand the benefits before supporting all blockstores, this feature will be scoped to S3 blockstore and only if presigned capability is enabled.

Initiation of Upload: The API will allow clients to initiate a multipart upload session, assigning a unique upload ID for subsequent operations. The client will require passing the number of parts it requires and the call will provide a set of URLs to upload each part.

Uploading Parts: Clients can upload individual parts of the file in parallel or in sequence. Each part will be based on the presigned URL provided by the initial call.

Support for Large Files: The API will handle files of substantial sizes, ensuring that large datasets can be uploaded without issues. Minimum part size will be 5M, except for the last block.

Data Integrity Checks: Each uploaded part will require the client to store the ETag as done today for presigned URL upload. The value of the ETag will be provided when the upload of all the parts is completed.

Pause and Resume: Clients can pause the upload and resume later, leveraging the upload ID to reinitiate the process. (TDB need to specify how long we will keep and if cleanup will be done)

Completion of Upload: Once all parts are uploaded, the client will send a request to complete the upload, address the file in lakeFS as a single file. Upload compete request will include the ETag for check part, in case of mismatch or fail to provide bad request code will return.

Cancelation: In order to cancel partial upload of multipart request, the client will require to call cancellation with the upload ID provided.

### Multipart upload capability using OpenAPI specification

**Paths for presign multipart upload operations**

```yaml
  /repositories/{repository}/branches/{branch}/staging/pmpu:
    parameters:
      - in: path
        name: repository
        required: true
        schema:
          type: string
      - in: path
        name: branch
        required: true
        schema:
          type: string
      - in: query
        name: path
        description: relative to the branch
        required: true
        schema:
          type: string
      - in: query
        name: presigned_parts
        description: number of presigned URL parts required to upload
        schema:
          type: integer
    post:
      tags:
        - experimental
      operationId: createPresignMultipartUpload
      summary: Initiate a presign multipart upload
      description: Initiates a presign multipart upload and returns an upload ID with presigned URLs for each part. Part numbers starts with 1. Each part except the last one has minimum size depends on the underlying blockstore implementation. For example working with S3 blockstore, minimum size is 5MB (excluding the last part).
      responses:
        200:
          description: Presign multipart upload initiated
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/PresignMultipartUpload"
        # ... other error responses

  /repositories/{repository}/branches/{branch}/staging/pmpu/{uploadId}:
      parameters:
      - in: path
        name: repository
        required: true
        schema:
          type: string
      - in: path
        name: branch
        required: true
        schema:
          type: string
      - in: path
        name: uploadId
        required: true
        schema:
          type: string
      - in: query
        name: path
        description: relative to the branch
        required: true
        schema:
          type: string
    put:
      tags:
        - experimental
      operationId: completePresignMultipartUpload
      summary: Complete a presign multipart upload
      description: Completes a presign multipart upload by assembling the uploaded parts.
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/CompleteMultipartUpload"
      responses:
        200:
          description: Presign multipart upload completed
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ObjectStats"
        # ... other error responses

    delete:
      tags:
        - experimental
      operationId: abortPresignMultipartUpload
      summary: Abort a presign multipart upload
      description: Aborts a presign multipart upload.
      responses:
        204:
          description: Presign multipart upload aborted

```


**Schemas used by operations**

```yaml
components:
  schemas:
    PresignMultipartUpload:
      type: object
      properties:
        upload_id:
          type: string
        physical_address:
          type: string
        presigned_urls:
          type: array
          items:
            type: string
      required:
        - upload_id
        - physical_address
        
    UploadPart:
      type: object
      properties:
        part_number:
          type: integer
        etag:
          type: string
      required:
        - part_number
        - etag
        
    CompletePresignMultipartUpload:
      type: object
      properties:
        physical_address:
          type: string
        parts:
          type: array
          description: "List of uploaded parts, should be ordered by ascending part number"
          items:
            $ref: "#/components/schemas/UploadPart"
        user_metadata:
          type: object
          additionalProperties:
            type: string
        content_type:
          type: string
          description: Object media type
      required:
        - physical_address
        - parts
```

### Support and discover

Presign support is a capability lakectl discover before switching to use presign for upload or download from lakeFS.
The presign multipart upload support will be part of the storage capability add `pre_sign_multipart_upload` optional field that when set to `true` the user can perform multipart upload using the new API.
lakeFS will return presign multipart support only on S3 with presign support enabled. There will be a configurable parameter `disable_pre_signed_multipart` under s3 block adapter that can be used to disable the capability if needed.

```yaml
    StorageConfig:
      type: object
      required:
        - blockstore_type
        - blockstore_namespace_example
        - blockstore_namespace_ValidityRegex
        - pre_sign_support
        - pre_sign_support_ui
        - import_support
        - import_validity_regex
      properties:
        blockstore_type:
          type: string
        blockstore_namespace_example:
          type: string
        blockstore_namespace_ValidityRegex:
          type: string
        default_namespace_prefix:
          type: string
        pre_sign_support:
          type: boolean
        pre_sign_support_ui:
          type: boolean
        import_support:
          type: boolean
        import_validity_regex:
          type: string
        pre_sign_multipart_upload:
          type: boolean
```

## Limitations

- **S3 block adapter exclusive:** Uploading files in multiple parts (multipart upload) is only available when using the S3 block adapter. This feature isn't currently supported with other storage options. This exclude the multipart upload support we provide by our S3 gateway where we provide implementation above each storage.
- **Part size and count restrictions:** There are limits on how you can split your file for upload:
    - **Maximum parts:** You can split your file into a maximum of 10000 parts.
    - **Minimum part size:** Each part must be at least 5MB in size. This is a temporary constraint and isn't currently configurable or discoverable. It will become an option when additional storage options are supported.
- **Initiating the upload:** When starting a multipart upload, you'll need to specify the total number of parts in your request. It will reduce the requests for each part's presigned URL when the client already knows the size.
- **Presigning part:** Request for a presigned URL of a specific part number will not be supported. This will block unknown size upload using this API.
- **Limited support:** Request for list upload parts will not be provided at this point.

## Next steps

None of the returned URLs has to be used, it is fine to ask for more than are needed.
In future we may add an _additional_ API call to URLs for uploading more parts.
This will allow more "streaming" uses, for instance as parallels to how Hadoop S3A uses the S3 MPU API and how the AWS SDKs upload manager handle streaming.
