package cmd

import (
	"context"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"slices"

	"github.com/hashicorp/go-retryablehttp"
)

var (
	// these errors aren't typed, so we match by regexp
	redirectsErrorRe  = regexp.MustCompile(`stopped after \d+ redirects\z`)
	schemeErrorRe     = regexp.MustCompile(`unsupported protocol scheme`)
	notTrustedErrorRe = regexp.MustCompile(`certificate is not trusted`)
)

func NewRetryClient(retriesCfg RetriesCfg, transport *http.Transport, checkRetry func(ctx context.Context, resp *http.Response, err error) (bool, error)) *http.Client {
	retryClient := retryablehttp.NewClient()
	if transport != nil {
		retryClient.HTTPClient.Transport = transport
	}
	retryClient.Logger = nil
	retryClient.RetryMax = int(retriesCfg.MaxAttempts)
	retryClient.RetryWaitMin = retriesCfg.MinWaitInterval
	retryClient.RetryWaitMax = retriesCfg.MaxWaitInterval
	retryClient.CheckRetry = checkRetry
	return retryClient.StandardClient()
}

// ShouldRetry checks if we should retry on this (HTTP) status.
type ShouldRetryer []int

func (s ShouldRetryer) Retry(status int) bool {
	return slices.Contains(s, status)
}

// MakeRetryPolicy makes a retry policy.
//
// - It will _never_ retry on these unrecoverable errors:
//
//   - a context error (typically canceled or deadline exceeded);
//   - invalid http scheme/protocol
//   - TLS cert validation failure
//
// - Any other error is retriable.
//
// - When there is no error, it will retry if should says to retry this status.
func CheckRetry(ctx context.Context, resp *http.Response, err error, should ShouldRetryer) (bool, error) {
	// do not retry on context.Canceled or context.DeadlineExceeded
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// handle client transport errors
	if err != nil {
		if v, ok := err.(*url.Error); ok {
			if redirectsErrorRe.MatchString(v.Error()) || // too many redirects
				schemeErrorRe.MatchString(v.Error()) || // invalid http scheme/protocol
				notTrustedErrorRe.MatchString(v.Error()) { // TLS cert verification failure
				return false, errors.Unwrap(v)
			}

			if _, ok := v.Err.(x509.UnknownAuthorityError); ok {
				return false, errors.Unwrap(v)
			}
		}
		// The standard http.Client wraps the above errors in a url.Error
		// They aren't retryable. Other errors are retryable.
		return true, nil
	}

	// handle HTTP response status code
	return should.Retry(resp.StatusCode), nil
}

// lakectl retry policy - we retry in the following cases:
// HTTP status 429 - too many requests
// HTTP status 500 - internal server error - could be recoverable
// HTTP status 503 - service unavailable
// We retry on all client transport errors except for:
//   - too many redirects
//   - invalid http scheme/protocol
//   - TLS cert verification failure
func lakectlRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	return CheckRetry(ctx, resp, err, lakectlDefaultRetryStatuses)
}

var lakectlDefaultRetryStatuses = ShouldRetryer{
	http.StatusTooManyRequests,
	http.StatusInternalServerError,
	http.StatusServiceUnavailable,
}
