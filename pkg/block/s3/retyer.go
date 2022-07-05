package s3

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
)

// errConnectionResetRetryer implements the `request.Retryer` interface and allows for
// customization of the retry behaviour of an AWS client.
// errConnectionResetRetryer was taken from cockroachdb
// https://github.com/cockroachdb/cockroach/blob/7a8cac9b11036dcf401453eb36e531fe381b70b4/pkg/cloud/amazon/s3_storage.go#L95
type errConnectionResetRetryer struct {
	client.DefaultRetryer
}

// isErrReadConnectionReset returns true if the underlying error is a read
// connection reset error.
//
// NB: A read connection reset error is thrown when the SDK is unable to read
// the response of an underlying API request due to a connection reset. The
// DefaultRetryer in the AWS SDK does not treat this error as a retryable error
// since the SDK does not have knowledge about the idempotence of the request,
// and whether it is safe to retry -
// https://github.com/aws/aws-sdk-go/pull/2926#issuecomment-553637658.
//
// In lakeFS all operations with s3 are considered idempotent,
// and so we can treat the read connection reset error as retryable too.
func isErrReadConnectionReset(err error) bool {
	// The error string must match the one in
	// github.com/aws/aws-sdk-go/aws/request/connection_reset_error.go. This is
	// unfortunate but the only solution until the SDK exposes a specialized error
	// code or type for this class of errors.
	return err != nil && strings.Contains(err.Error(), "read: connection reset")
}

// ShouldRetry implements the request.Retryer interface.
func (sr *errConnectionResetRetryer) ShouldRetry(r *request.Request) bool {
	return sr.DefaultRetryer.ShouldRetry(r) || isErrReadConnectionReset(r.Error)
}
