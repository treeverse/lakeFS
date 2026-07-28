# CompletePresignMultipartUpload

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**physical_address** | **String** |  | 
**parts** | [**Vec<models::UploadPart>**](UploadPart.md) | List of uploaded parts, should be ordered by ascending part number | 
**user_metadata** | Option<**std::collections::HashMap<String, String>**> |  | [optional]
**content_type** | Option<**String**> | Object media type | [optional]
**checksum_algorithm** | Option<[**models::ChecksumAlgorithm**](ChecksumAlgorithm.md)> |  | [optional]
**checksum_type** | Option<[**models::ChecksumType**](ChecksumType.md)> |  | [optional]
**checksum** | Option<**String**> | Base64-encoded big-endian full-object checksum of the entire object content, computed with checksum_algorithm (S3 encoding convention). Requires checksum_algorithm. lakeFS compares the storage-computed full-object checksum of the assembled object against this value and fails the completion on mismatch; a successful completion means the checksum was validated.  | [optional]
**mpu_object_size** | Option<**i64**> | Expected total size in bytes of the assembled object, validated by the storage on completion. May be supplied with or without a checksum.  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


