# ChecksumAlgorithm

Checksum algorithm for full-object integrity validation of a presign multipart upload. Supported only when the storage configuration reports pre_sign_multipart_upload_checksum. Only CRC64NVME is supported: parts are uploaded through unmodified presigned URLs, and the storage computes a full-object CRC64NVME by default, which lakeFS compares against the client-supplied value on completion. Other algorithms require per-part checksum headers (composite mode) and are not supported. 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


