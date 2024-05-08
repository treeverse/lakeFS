package cmd

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	tooManyRedirectsErrorMessage = "stopped after 5 redirects"
	someRandomErrorMessage       = "random error message"
	testURL                      = "https://example.com"
)

func TestLakectlRetryPolicy(t *testing.T) {
	testCases := []struct {
		name                string
		getTestContext      func() context.Context
		resp                *http.Response
		err                 error
		expectedShouldRetry bool
		expectedError       string
	}{
		{
			name: "Context Error - Context Deadline Exceeded",
			getTestContext: func() context.Context {
				ctx := context.Background()
				ctx, c := context.WithDeadline(ctx, time.Now().Add(-7*time.Hour))
				c()
				return ctx
			},
			resp:                nil,
			err:                 nil,
			expectedShouldRetry: false,
			expectedError:       context.DeadlineExceeded.Error(),
		},
		{
			name: "Context Error - Context Cancelation",
			getTestContext: func() context.Context {
				ctx := context.Background()
				ctx, c := context.WithCancel(ctx)
				c()
				return ctx
			},
			resp:                nil,
			err:                 nil,
			expectedShouldRetry: false,
			expectedError:       context.Canceled.Error(),
		},
		{
			name: "Transport Error - Too Many Redirects",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp:                nil,
			err:                 &url.Error{Op: http.MethodGet, URL: testURL, Err: errors.New(tooManyRedirectsErrorMessage)},
			expectedShouldRetry: false,
			expectedError:       tooManyRedirectsErrorMessage,
		},
		{
			name: "Transport Error - Random Transport Error",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp:                nil,
			err:                 &url.Error{Op: http.MethodGet, URL: testURL, Err: errors.New(someRandomErrorMessage)},
			expectedShouldRetry: true,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 429 Too Many Requests",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
			},
			err:                 nil,
			expectedShouldRetry: true,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 500 Internal Server Error",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusInternalServerError,
			},
			err:                 nil,
			expectedShouldRetry: true,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 503 Service Unavailable",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusServiceUnavailable,
			},
			err:                 nil,
			expectedShouldRetry: true,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 401 Unauthorized",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusUnauthorized,
			},
			err:                 nil,
			expectedShouldRetry: false,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 404 Not Found",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusNotFound,
			},
			err:                 nil,
			expectedShouldRetry: false,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 200 Ok",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusOK,
			},
			err:                 nil,
			expectedShouldRetry: false,
			expectedError:       "",
		},
		{
			name: "HTTP Status - 201 Created",
			getTestContext: func() context.Context {
				return context.Background()
			},
			resp: &http.Response{
				StatusCode: http.StatusCreated,
			},
			err:                 nil,
			expectedShouldRetry: false,
			expectedError:       "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			shouldRetry, err := lakectlRetryPolicy(tc.getTestContext(), tc.resp, tc.err)
			require.Equal(t, tc.expectedShouldRetry, shouldRetry)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}
