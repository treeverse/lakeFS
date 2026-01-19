package sig

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/sig/sigtest"
)

func TestSigV4VerifyExpirationPresigned(t *testing.T) {
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name          string
		expires       int64
		requestTime   time.Time
		expectedError error
	}{
		{
			name:          "valid presigned URL",
			expires:       3600,
			requestTime:   now,
			expectedError: nil,
		},
		{
			name:          "expired presigned URL",
			expires:       10,
			requestTime:   now.Add(-1 * time.Minute),
			expectedError: errors.ErrExpiredPresignRequest,
		},
		{
			name:          "presigned URL with zero expiration",
			expires:       0,
			requestTime:   now.Add(-1 * time.Second),
			expectedError: errors.ErrExpiredPresignRequest,
		},
		{
			name:          "presigned URL at max expiration",
			expires:       int64(AmzPresignMaxExpires),
			requestTime:   now,
			expectedError: nil,
		},
		{
			name:          "presigned URL future request beyond clock skew",
			expires:       3600,
			requestTime:   now.Add(AmzMaxClockSkew + 1*time.Minute),
			expectedError: errors.ErrRequestNotReadyYet,
		},
		{
			name:          "presigned URL future request within clock skew",
			expires:       3600,
			requestTime:   now.Add(AmzMaxClockSkew / 2),
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a minimal verification context - verifyExpiration only needs the X-Amz-Date and Expiry parameters
			amzDate := tc.requestTime.Format(v4timeFormat)
			dateStamp := tc.requestTime.Format(v4shortTimeFormat)

			req, _ := http.NewRequest(http.MethodGet, "https://example.com/test", nil)

			// Set X-Amz-Date in query for presigned URLs
			query := url.Values{}
			query.Set("X-Amz-Date", amzDate)
			req.URL.RawQuery = query.Encode()

			ctx := &verificationCtx{
				Request: req,
				Query:   req.URL.Query(),
				AuthValue: V4Auth{
					Date:        dateStamp,
					IsPresigned: true,
					Expires:     tc.expires,
				},
			}

			err := ctx.verifyExpiration(now)
			require.ErrorIs(t, err, tc.expectedError)
		})
	}
}

func TestSigV4VerifyExpirationNonPresigned(t *testing.T) {
	// Test non-presigned requests using shared clock skew test cases
	testCases := sigtest.CommonClockSkewTestCases(AmzMaxClockSkew)
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			requestTime := now.Add(tc.Offset)
			// Create a minimal verification context - verifyExpiration only needs the X-Amz-Date header
			amzDate := requestTime.Format(v4timeFormat)
			dateStamp := requestTime.Format(v4shortTimeFormat)

			req, _ := http.NewRequest(http.MethodGet, "https://example.com/test", nil)
			req.Header.Set("X-Amz-Date", amzDate)

			ctx := &verificationCtx{
				Request:   req,
				Query:     req.URL.Query(),
				AuthValue: V4Auth{Date: dateStamp},
			}

			err := ctx.verifyExpiration(now)
			require.ErrorIs(t, err, tc.ExpectedError)
		})
	}
}

func TestGetAmzDate(t *testing.T) {
	requestTime := time.Date(2025, 12, 15, 12, 30, 45, 0, time.UTC)
	validAmzDate := requestTime.Format(v4timeFormat)             // "20240615T123045Z"
	validCredentialDate := requestTime.Format(v4shortTimeFormat) // "20240615"

	type header struct {
		name  string
		value string
	}

	testCases := []struct {
		name           string
		dateParam      string
		dateHeaders    []header
		credentialDate string
		expectedDate   string
		expectedError  error
	}{
		{
			name:           "valid X-Amz-Date query param with matching credential date",
			dateParam:      validAmzDate,
			credentialDate: validCredentialDate,
			expectedDate:   validAmzDate,
			expectedError:  nil,
		},
		{
			name:           "valid x-amz-date header with matching credential date",
			dateHeaders:    []header{{name: "x-amz-date", value: validAmzDate}},
			credentialDate: validCredentialDate,
			expectedDate:   validAmzDate,
			expectedError:  nil,
		},
		{
			name:           "valid date header fallback with matching credential date",
			dateHeaders:    []header{{name: "date", value: validAmzDate}},
			credentialDate: validCredentialDate,
			expectedDate:   validAmzDate,
			expectedError:  nil,
		},
		{
			name:           "query param takes precedence over header",
			dateParam:      validAmzDate,
			dateHeaders:    []header{{name: "x-amz-date", value: "20200101T000000Z"}},
			credentialDate: validCredentialDate,
			expectedDate:   validAmzDate,
			expectedError:  nil,
		},
		{
			name:           "missing date header",
			dateHeaders:    []header{},
			credentialDate: validCredentialDate,
			expectedDate:   "",
			expectedError:  errors.ErrMissingDateHeader,
		},
		{
			name:           "malformed X-Amz-Date - wrong format",
			dateHeaders:    []header{{name: "x-amz-date", value: "2024-12-15T12:00:00Z"}},
			credentialDate: validCredentialDate,
			expectedDate:   "",
			expectedError:  errors.ErrMalformedDate,
		},
		{
			name:           "malformed X-Amz-Date - invalid string",
			dateHeaders:    []header{{name: "x-amz-date", value: "not-a-date"}},
			credentialDate: validCredentialDate,
			expectedDate:   "",
			expectedError:  errors.ErrMalformedDate,
		},
		{
			name:           "malformed credential date - wrong format",
			dateHeaders:    []header{{name: "x-amz-date", value: validAmzDate}},
			credentialDate: "2024-12-15", // wrong format
			expectedDate:   "",
			expectedError:  errors.ErrMalformedCredentialDate,
		},
		{
			name:           "malformed credential date - empty",
			dateHeaders:    []header{{name: "x-amz-date", value: validAmzDate}},
			credentialDate: "",
			expectedDate:   "",
			expectedError:  errors.ErrMalformedCredentialDate,
		},
		{
			name:           "credential date one day earlier than X-Amz-Date",
			dateHeaders:    []header{{name: "x-amz-date", value: validAmzDate}},
			credentialDate: requestTime.Add(-24 * time.Hour).Format(v4shortTimeFormat),
			expectedDate:   "",
			expectedError:  errors.ErrInvalidCredentialDate,
		},
		{
			name:           "credential date one day later than X-Amz-Date",
			dateHeaders:    []header{{name: "x-amz-date", value: validAmzDate}},
			credentialDate: requestTime.Add(24 * time.Hour).Format(v4shortTimeFormat),
			expectedDate:   "",
			expectedError:  errors.ErrInvalidCredentialDate,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "https://example.com/test", nil)
			if tc.dateParam != "" {
				req.URL.RawQuery = "X-Amz-Date=" + tc.dateParam
			}

			for _, header := range tc.dateHeaders {
				req.Header.Set(header.name, header.value)
			}

			ctx := &verificationCtx{
				Request: req,
				Query:   req.URL.Query(),
				AuthValue: V4Auth{
					Date: tc.credentialDate,
				},
			}

			amzDate, err := ctx.getAmzDate()

			require.ErrorIs(t, err, tc.expectedError)
			require.Equal(t, tc.expectedDate, amzDate)
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

			require.ErrorIs(t, err, tc.expectedError)
			require.Equal(t, value, tc.expectedValue)
		})
	}
}

func TestIsV4PresignedRequest(t *testing.T) {
	testCases := []struct {
		name          string
		query         url.Values
		expectedError error
	}{
		{
			name: "valid V4 presigned request",
			query: url.Values{
				v4AmzAlgorithm:     {"AWS4-HMAC-SHA256"},
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {"3600"},
			},
			expectedError: nil,
		},
		{
			name: "missing algorithm - not a V4 request",
			query: url.Values{
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {"3600"},
			},
			expectedError: ErrBadAuthorizationFormat,
		},
		{
			name: "invalid algorithm",
			query: url.Values{
				v4AmzAlgorithm:     {"AWS4-HMAC-SHA512"},
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {"3600"},
			},
			expectedError: errors.ErrInvalidQuerySignatureAlgo,
		},
		{
			name: "missing required param (credential)",
			query: url.Values{
				v4AmzAlgorithm:     {"AWS4-HMAC-SHA256"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
				v4AmzExpires:       {"3600"},
			},
			expectedError: errors.ErrMissingFields,
		},
		{
			name: "missing required param (expires)",
			query: url.Values{
				v4AmzAlgorithm:     {"AWS4-HMAC-SHA256"},
				v4AmzCredential:    {"AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request"},
				v4AmzSignature:     {"abcd1234"},
				v4AmzDate:          {"20230101T120000Z"},
				v4AmzSignedHeaders: {"host"},
			},
			expectedError: errors.ErrMissingFields,
		},
		{
			name:          "empty query params - not a V4 request",
			query:         url.Values{},
			expectedError: ErrBadAuthorizationFormat,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := isV4PresignedRequest(tc.query)
			require.ErrorIs(t, err, tc.expectedError)
		})
	}
}
