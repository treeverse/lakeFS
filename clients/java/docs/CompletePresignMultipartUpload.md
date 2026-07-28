

# CompletePresignMultipartUpload


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**physicalAddress** | **String** |  |  |
|**parts** | [**List&lt;UploadPart&gt;**](UploadPart.md) | List of uploaded parts, should be ordered by ascending part number |  |
|**userMetadata** | **Map&lt;String, String&gt;** |  |  [optional] |
|**contentType** | **String** | Object media type |  [optional] |
|**checksumAlgorithm** | **ChecksumAlgorithm** |  |  [optional] |
|**checksumType** | **ChecksumType** |  |  [optional] |
|**checksum** | **String** | Base64-encoded big-endian full-object checksum of the entire object content, computed with checksum_algorithm (S3 encoding convention). Requires checksum_algorithm. lakeFS compares the storage-computed full-object checksum of the assembled object against this value and fails the completion on mismatch; a successful completion means the checksum was validated.  |  [optional] |
|**mpuObjectSize** | **Long** | Expected total size in bytes of the assembled object, validated by the storage on completion. May be supplied with or without a checksum.  |  [optional] |



