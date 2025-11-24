package sig

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/gateway/errors"
)

func TestVerifyExpiration(t *testing.T) {
	now := time.Now().UTC()

	testCases := []struct {
		name          string
		isPresigned   bool
		expires       int64
		requestTime   time.Time
		expectedError error
	}{
		// Presigned URL tests
		{
			name:          "valid presigned URL",
			isPresigned:   true,
			expires:       3600,
			requestTime:   now,
			expectedError: nil,
		},
		{
			name:          "expired presigned URL",
			isPresigned:   true,
			expires:       10,
			requestTime:   now.Add(-1 * time.Minute),
			expectedError: errors.ErrExpiredPresignRequest,
		},
		{
			name:          "presigned URL with zero expiration",
			isPresigned:   true,
			expires:       0,
			requestTime:   now.Add(-1 * time.Second),
			expectedError: errors.ErrExpiredPresignRequest,
		},
		{
			name:          "presigned URL at max expiration (7 days)",
			isPresigned:   true,
			expires:       604800,
			requestTime:   now,
			expectedError: nil,
		},
		{
			name:          "presigned URL future request beyond clock skew",
			isPresigned:   true,
			expires:       3600,
			requestTime:   now.Add(10 * time.Minute),
			expectedError: errors.ErrRequestNotReadyYet,
		},
		{
			name:          "presigned URL future request within clock skew",
			isPresigned:   true,
			expires:       3600,
			requestTime:   now.Add(3 * time.Minute),
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a minimal verification context
			amzDate := tc.requestTime.Format(v4timeFormat)
			// Use the same date for the credential scope (must match calendar day)
			dateStamp := tc.requestTime.Format(v4shortTimeFormat)

			req, _ := http.NewRequest(http.MethodGet, "https://example.com/test", nil)

			// Set X-Amz-Date in query for presigned URLs
			if tc.isPresigned {
				query := url.Values{}
				query.Set("X-Amz-Date", amzDate)
				req.URL.RawQuery = query.Encode()
			}

			ctx := &verificationCtx{
				Request: req,
				Query:   req.URL.Query(),
				AuthValue: V4Auth{
					Date:        dateStamp,
					IsPresigned: tc.isPresigned,
					Expires:     tc.expires,
				},
			}

			err := ctx.verifyExpiration()

			if tc.expectedError == nil {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err != tc.expectedError {
					t.Errorf("expected error %v, got %v", tc.expectedError, err)
				}
			}
		})
	}
}

func TestParseExpires(t *testing.T) {
	testCases := []struct {
		name          string
		expiresStr    string
		expectedValue int64
		expectedError error
	}{
		{
			name:          "valid expires",
			expiresStr:    "3600",
			expectedValue: 3600,
			expectedError: nil,
		},
		{
			name:          "zero expires",
			expiresStr:    "0",
			expectedValue: 0,
			expectedError: nil,
		},
		{
			name:          "max expires (7 days)",
			expiresStr:    "604800",
			expectedValue: 604800,
			expectedError: nil,
		},
		{
			name:          "negative expires",
			expiresStr:    "-100",
			expectedValue: 0,
			expectedError: errors.ErrNegativeExpires,
		},
		{
			name:          "expires over 7 days",
			expiresStr:    "604801",
			expectedValue: 0,
			expectedError: errors.ErrMaximumExpires,
		},
		{
			name:          "malformed expires - not a number",
			expiresStr:    "abc",
			expectedValue: 0,
			expectedError: errors.ErrMalformedExpires,
		},
		{
			name:          "malformed expires - decimal",
			expiresStr:    "3600.5",
			expectedValue: 0,
			expectedError: errors.ErrMalformedExpires,
		},
		{
			name:          "malformed expires - empty string",
			expiresStr:    "",
			expectedValue: 0,
			expectedError: errors.ErrMalformedExpires,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := parseExpires(tc.expiresStr)

			if value != tc.expectedValue {
				t.Errorf("expected value %d, got %d", tc.expectedValue, value)
			}

			if tc.expectedError == nil {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err != tc.expectedError {
					t.Errorf("expected error %v, got %v", tc.expectedError, err)
				}
			}
		})
	}
}

func TestHasRequiredParams(t *testing.T) {
	testCases := []struct {
		name     string
		query    url.Values
		expected bool
	}{
		{
			name: "all required params present",
			query: url.Values{
				v4AmzAlgorithm:     {"AWS4-HMAC-SHA256"},
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {"3600"},
			},
			expected: true,
		},
		{
			name: "missing a parameter",
			query: url.Values{
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {"3600"},
			},
			expected: false,
		},
		{
			name: "parameter is present but empty",
			query: url.Values{
				v4AmzAlgorithm:     {"AWS4-HMAC-SHA256"},
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {""},
			},
			expected: true, // Key exists in map, even if empty
		},
		{
			name:     "empty query params",
			query:    url.Values{},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := hasRequiredParams(tc.query)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
