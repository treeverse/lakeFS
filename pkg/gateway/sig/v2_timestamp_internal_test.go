package sig

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/sig/sigtest"
)

func TestV2VerifyRequestDate(t *testing.T) {
	// Use shared clock skew test cases
	testCases := sigtest.CommonClockSkewTestCases(AmzMaxClockSkew)
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// Create minimal request - verifyRequestDate only needs the X-Amz-Date header
			req, err := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/examplebucket/test.txt", nil)
			require.NoError(t, err, "failed to create request")

			requestTime := now.Add(tc.Offset)
			req.Header.Set("X-Amz-Date", requestTime.Format("20060102T150405Z"))

			authenticator := NewV2SigAuthenticator(req, "s3.amazonaws.com")

			err = authenticator.verifyRequestDate(now)
			require.ErrorIs(t, err, tc.ExpectedError)
		})
	}
}

func TestV2DateHeaderSelection(t *testing.T) {
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name           string
		requestTime    time.Time
		useDateHeader  bool
		setBothHeaders bool
		expectedError  error
	}{
		{
			name:          "valid request with Date header",
			requestTime:   now,
			useDateHeader: true,
			expectedError: nil,
		},
		{
			name:           "X-Amz-Date takes precedence when both headers present",
			requestTime:    now,
			useDateHeader:  false,
			setBothHeaders: true,
			expectedError:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create minimal request - verifyRequestDate only needs the X-Amz-Date or Date header
			req, err := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/examplebucket/test.txt", nil)
			require.NoError(t, err, "failed to create request")

			if tc.useDateHeader {
				req.Header.Set("Date", tc.requestTime.Format(time.RFC1123))
			} else {
				req.Header.Set("X-Amz-Date", tc.requestTime.Format("20060102T150405Z"))

				// For precedence test: set an expired Date header that would fail validation
				// If X-Amz-Date takes precedence (as it should), the request will succeed
				if tc.setBothHeaders {
					expiredDate := now.Add(-AmzMaxClockSkew - 1*time.Hour)
					req.Header.Set("Date", expiredDate.Format(time.RFC1123))
				}
			}

			authenticator := NewV2SigAuthenticator(req, "s3.amazonaws.com")

			err = authenticator.verifyRequestDate(now)
			require.ErrorIs(t, err, tc.expectedError, "failed to verify signature")
		})
	}
}

func TestV2InvalidDateHeader(t *testing.T) {
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)
	testCases := []struct {
		name          string
		date          string
		expectedError error
	}{
		{
			name:          "missing date header",
			date:          "",
			expectedError: gatewayerrors.ErrMissingFields,
		},
		{
			name:          "malformed date header",
			date:          "not-a-valid-date-format",
			expectedError: gatewayerrors.ErrMalformedDate,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create minimal request - verifyRequestDate only needs the X-Amz-Date or Date header
			req, err := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/examplebucket/test.txt", nil)
			require.NoError(t, err, "failed to create request")

			if tc.date != "" {
				req.Header.Set("X-Amz-Date", tc.date)
			}

			authenticator := NewV2SigAuthenticator(req, "s3.amazonaws.com")

			err = authenticator.verifyRequestDate(now)
			require.ErrorIs(t, err, tc.expectedError)
		})
	}
}
