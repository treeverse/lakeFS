package cmd

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

var (
	// these errors aren't typed, so we match by regexp
	redirectsErrorRe  = regexp.MustCompile(`stopped after \d+ redirects\z`)
	schemeErrorRe     = regexp.MustCompile(`unsupported protocol scheme`)
	notTrustedErrorRe = regexp.MustCompile(`certificate is not trusted`)
)

func NewRetryClient(maxRetries int, retryWaitMin, retryWaitMax time.Duration, transport *http.Transport) *http.Client {
	retryClient := retryablehttp.NewClient()
	if transport != nil {
		retryClient.HTTPClient.Transport = transport
	}
	retryClient.RetryMax = maxRetries
	retryClient.RetryWaitMin = retryWaitMin
	retryClient.RetryWaitMax = retryWaitMax
	retryClient.CheckRetry = lakectlRetryPolicy
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
				return false, v
			}

			if _, ok := v.Err.(x509.UnknownAuthorityError); ok {
				return false, v
			}
		}
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
