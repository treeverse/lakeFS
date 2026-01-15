package sig

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/sig/sigtest"
)

func TestJavaV2VerifyRequestDate(t *testing.T) {
	// Use shared clock skew test cases
	testCases := sigtest.CommonClockSkewTestCases(AmzMaxClockSkew)
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			timestamp := now.Add(tc.Offset)

			// Create minimal request - verifyRequestDate only needs the Timestamp parameter
			u, err := url.Parse("https://s3.amazonaws.com/examplebucket/test.txt")
			require.NoError(t, err)

			q := u.Query()
			q.Set("Timestamp", timestamp.Format("2006-01-02T15:04:05.000Z"))
			u.RawQuery = q.Encode()

			req, err := http.NewRequest(http.MethodGet, u.String(), nil)
			require.NoError(t, err)

			authenticator := NewJavaV2SigAuthenticator(req, u.Host)

			err = authenticator.verifyRequestDate(now)
			require.ErrorIs(t, err, tc.ExpectedError)
		})
	}
}

func TestJavaV2InvalidTimestamp(t *testing.T) {
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name          string
		timestamp     string
		expectedError error
	}{
		{
			name:          "missing timestamp parameter",
			timestamp:     "",
			expectedError: gatewayerrors.ErrMissingFields,
		},
		{
			name:          "malformed timestamp parameter",
			timestamp:     "not-a-valid-timestamp",
			expectedError: gatewayerrors.ErrMalformedDate,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create minimal request - verifyRequestDate only needs the Timestamp parameter
			u, err := url.Parse("https://s3.amazonaws.com/examplebucket/test.txt")
			require.NoError(t, err)

			q := u.Query()
			if tc.timestamp != "" {
				q.Set("Timestamp", tc.timestamp)
			}
			u.RawQuery = q.Encode()

			req, err := http.NewRequest(http.MethodGet, u.String(), nil)
			require.NoError(t, err, "failed to create request")
			authenticator := NewJavaV2SigAuthenticator(req, u.Host)

			err = authenticator.verifyRequestDate(now)
			require.ErrorIs(t, err, tc.expectedError)
		})
	}
}
