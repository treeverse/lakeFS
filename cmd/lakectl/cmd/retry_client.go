package cmd

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"

	"github.com/hashicorp/go-retryablehttp"
)

var (
	// these errors aren't typed, so we match by regexp
	redirectsErrorRe  = regexp.MustCompile(`stopped after \d+ redirects\z`)
	schemeErrorRe     = regexp.MustCompile(`unsupported protocol scheme`)
	notTrustedErrorRe = regexp.MustCompile(`certificate is not trusted`)

	// We need to consume response bodies to maintain http connections, but
	// limit the size we consume to respReadLimit.
	respReadLimit = int64(4096) //nolint:mnd
)

func NewRetryClient(retriesCfg RetriesCfg, transport *http.Transport) *http.Client {
	retryClient := retryablehttp.NewClient()
	if transport != nil {
		retryClient.HTTPClient.Transport = transport
	}
	retryClient.Logger = nil
	retryClient.RetryMax = retriesCfg.MaxAttempts
	retryClient.RetryWaitMin = retriesCfg.MinWaitInterval
	retryClient.RetryWaitMax = retriesCfg.MaxWaitInterval
	retryClient.CheckRetry = lakectlRetryPolicy
	retryClient.ErrorHandler = customErrorHandler
	return retryClient.StandardClient()
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
		// The stblib http.Client wraps the above errors in a url.Error
		// They aren't retryable. Other errors are retryable.
		return true, nil
	}

	// handle HTTP response status code
	if resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusInternalServerError ||
		resp.StatusCode == http.StatusServiceUnavailable {
		return true, nil
	}
	return false, nil
}

func customErrorHandler(resp *http.Response, err error, _ int) (*http.Response, error) {
	if resp != nil {
		defer resp.Body.Close()
		io.Copy(io.Discard, io.LimitReader(resp.Body, respReadLimit)) //nolint:errcheck
	}
	return resp, err
}
