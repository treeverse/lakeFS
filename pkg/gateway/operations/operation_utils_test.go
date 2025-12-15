package operations

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAmzMetaAsMetadata_KeyLowercasing(t *testing.T) {
	tests := []struct {
		name         string
		headers      map[string][]string
		expectedKeys map[string]string
	}{
		{
			name: "title-case keys from Go canonicalization are lowercased",
			headers: map[string][]string{
				// These simulate what Go's HTTP server produces after canonicalization
				// When a client sends "X-Amz-Meta-FOO", Go canonicalizes to "X-Amz-Meta-Foo"
				"X-Amz-Meta-Foo":       {"bar"},
				"X-Amz-Meta-Mykey":     {"myvalue"},
				"X-Amz-Meta-Allcaps":   {"capsvalue"},
				"X-Amz-Meta-Mixedcase": {"mixed"},
			},
			expectedKeys: map[string]string{
				"X-Amz-Meta-foo":       "bar",
				"X-Amz-Meta-mykey":     "myvalue",
				"X-Amz-Meta-allcaps":   "capsvalue",
				"X-Amz-Meta-mixedcase": "mixed",
			},
		},
		{
			name: "already lowercase keys remain lowercase",
			headers: map[string][]string{
				"X-Amz-Meta-Lowercase": {"value1"},
				"X-Amz-Meta-Another":   {"value2"},
			},
			expectedKeys: map[string]string{
				"X-Amz-Meta-lowercase": "value1",
				"X-Amz-Meta-another":   "value2",
			},
		},
		{
			name: "non-metadata headers are ignored",
			headers: map[string][]string{
				"X-Amz-Meta-Key": {"metavalue"},
				"Content-Type":   {"application/json"},
				"Authorization":  {"Bearer token"},
			},
			expectedKeys: map[string]string{
				"X-Amz-Meta-key": "metavalue",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("PUT", "http://example.com/test", nil)
			require.NoError(t, err)

			// Set headers directly on the request's Header map
			// This bypasses Go's automatic canonicalization during header parsing
			for k, v := range tt.headers {
				req.Header[k] = v
			}

			metadata, err := amzMetaAsMetadata(req)
			require.NoError(t, err)

			// Verify we got exactly the expected number of keys
			require.Len(t, metadata, len(tt.expectedKeys), "unexpected number of metadata keys")

			// Verify each expected key and value
			for expectedKey, expectedValue := range tt.expectedKeys {
				actualValue, exists := metadata[expectedKey]
				require.True(t, exists, "expected key %q not found in metadata", expectedKey)
				require.Equal(t, expectedValue, actualValue, "value mismatch for key %q", expectedKey)
			}
		})
	}
}

func TestAmzMetaAsMetadata_RFC2047Decoding(t *testing.T) {
	tests := []struct {
		name          string
		headerKey     string
		headerValue   string
		expectedKey   string
		expectedValue string
	}{
		{
			name:          "RFC 2047 encoded value is decoded and key is lowercased",
			headerKey:     "X-Amz-Meta-Nonascii",                  // Title-case after Go canonicalization
			headerValue:   "=?utf-8?q?=D7=A9=D7=9C=D7=95=D7=9D?=", // Hebrew "shalom"
			expectedKey:   "X-Amz-Meta-nonascii",
			expectedValue: "שלום",
		},
		{
			name:          "Plain ASCII value with title-case key",
			headerKey:     "X-Amz-Meta-Simple", // Title-case after Go canonicalization
			headerValue:   "plain text",
			expectedKey:   "X-Amz-Meta-simple",
			expectedValue: "plain text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("PUT", "http://example.com/test", nil)
			require.NoError(t, err)

			// Set headers as they would appear after Go's HTTP canonicalization
			req.Header.Set(tt.headerKey, tt.headerValue)

			metadata, err := amzMetaAsMetadata(req)
			require.NoError(t, err)

			actualValue, exists := metadata[tt.expectedKey]
			require.True(t, exists, "expected key %q not found", tt.expectedKey)
			require.Equal(t, tt.expectedValue, actualValue)
		})
	}
}
