package s3

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// extractSSHeaderUploadPart extracts the x-amz-server-side-* headers from the given
// UploadPartOutput response.
func extractSSHeaderUploadPart(resp *s3.UploadPartOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSECustomerAlgorithm != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", *resp.SSECustomerAlgorithm)
	}
	if resp.SSECustomerKeyMD5 != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", *resp.SSECustomerKeyMD5)
	}
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	return headers
}

// extractSSHeaderUploadPartCopy extracts the x-amz-server-side-* headers from the given
// UploadPartCopyOutput response.
func extractSSHeaderUploadPartCopy(resp *s3.UploadPartCopyOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSECustomerAlgorithm != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", *resp.SSECustomerAlgorithm)
	}
	if resp.SSECustomerKeyMD5 != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", *resp.SSECustomerKeyMD5)
	}
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	return headers
}

// extractSSHeaderCreateMultipartUpload extracts the x-amz-server-side-* headers from the given
// CreateMultipartUploadOutput response.
func extractSSHeaderCreateMultipartUpload(resp *s3.CreateMultipartUploadOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSECustomerAlgorithm != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", *resp.SSECustomerAlgorithm)
	}
	if resp.SSECustomerKeyMD5 != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", *resp.SSECustomerKeyMD5)
	}
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	if resp.SSEKMSEncryptionContext != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Context", *resp.SSEKMSEncryptionContext)
	}
	return headers
}

// extractSSHeaderCompleteMultipartUpload extracts the x-amz-server-side-* headers from the given
// CompleteMultipartUploadOutput response.
func extractSSHeaderCompleteMultipartUpload(resp *s3.CompleteMultipartUploadOutput) http.Header {
	// x-amz-server-side-* headers
	headers := make(http.Header)
	if resp.SSEKMSKeyId != nil {
		headers.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", *resp.SSEKMSKeyId)
	}
	if resp.ServerSideEncryption != "" {
		headers.Set("X-Amz-Server-Side-Encryption", string(resp.ServerSideEncryption))
	}
	return headers
}
