package operations

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
)

func TestIsValidMetadataKey(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		valid bool
	}{
		// Empty key
		{name: "empty key", key: "", valid: false},

		// Basic alphanumeric
		{name: "lowercase letters", key: "abc", valid: true},
		{name: "uppercase letters", key: "ABC", valid: true},
		{name: "mixed case", key: "AbC", valid: true},
		{name: "digits", key: "123", valid: true},
		{name: "alphanumeric", key: "abc123", valid: true},

		// Special characters that are valid HTTP token chars (RFC 7230) and S3 accepts
		{name: "hyphen", key: "test-key", valid: true},
		{name: "underscore", key: "test_key", valid: true},
		{name: "dot", key: "test.key", valid: true},
		{name: "hash", key: "test#key", valid: true},
		{name: "exclamation", key: "test!key", valid: true},
		{name: "dollar", key: "test$key", valid: true},
		{name: "percent", key: "test%key", valid: true},
		{name: "ampersand", key: "test&key", valid: true},
		{name: "single quote", key: "test'key", valid: true},
		{name: "asterisk", key: "test*key", valid: true},
		{name: "plus", key: "test+key", valid: true},
		{name: "caret", key: "test^key", valid: true},
		{name: "backtick", key: "test`key", valid: true},
		{name: "pipe", key: "test|key", valid: true},
		{name: "tilde", key: "test~key", valid: true},

		// Characters that should be rejected
		// Note: ( and ) are accepted by S3 but rejected by Go's HTTP server (not valid token chars)
		{name: "open paren", key: "test(key", valid: false},
		{name: "close paren", key: "test)key", valid: false},
		{name: "space", key: "test key", valid: false},
		{name: "slash", key: "test/key", valid: false},
		{name: "colon", key: "test:key", valid: false},
		{name: "semicolon", key: "test;key", valid: false},
		{name: "less than", key: "test<key", valid: false},
		{name: "equals", key: "test=key", valid: false},
		{name: "greater than", key: "test>key", valid: false},
		{name: "question mark", key: "test?key", valid: false},
		{name: "at sign", key: "test@key", valid: false},
		{name: "open bracket", key: "test[key", valid: false},
		{name: "backslash", key: "test\\key", valid: false},
		{name: "close bracket", key: "test]key", valid: false},
		{name: "open brace", key: "test{key", valid: false},
		{name: "close brace", key: "test}key", valid: false},
		{name: "double quote", key: "test\"key", valid: false},
		{name: "comma", key: "test,key", valid: false},

		// Control characters
		{name: "null byte", key: "test\x00key", valid: false},
		{name: "tab", key: "test\tkey", valid: false},
		{name: "newline", key: "test\nkey", valid: false},
		{name: "carriage return", key: "test\rkey", valid: false},
		{name: "delete", key: "test\x7fkey", valid: false},

		// Non-ASCII characters
		{name: "unicode", key: "testékey", valid: false},
		{name: "high byte", key: "test\x80key", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidMetadataKey(tt.key)
			require.Equal(t, tt.valid, got, "isValidMetadataKey(%q) = %v, want %v", tt.key, got, tt.valid)
		})
	}
}

func TestAmzMetaAsMetadata_KeyLowercasing(t *testing.T) {
	tests := []struct {
		name         string
		headers      map[string]string
		expectedKeys map[string]string
	}{
		{
			name: "title-case keys from Go canonicalization are lowercased",
			headers: map[string]string{
				// These simulate what Go's HTTP server produces after canonicalization
				// When a client sends "X-Amz-Meta-FOO", Go canonicalizes to "X-Amz-Meta-Foo"
				"X-Amz-Meta-Foo":       "bar",
				"X-Amz-Meta-Mykey":     "myvalue",
				"X-Amz-Meta-Allcaps":   "capsvalue",
				"X-Amz-Meta-Mixedcase": "mixed",
			},
			expectedKeys: map[string]string{
				"X-Amz-Meta-foo":       "bar",
				"X-Amz-Meta-mykey":     "myvalue",
				"X-Amz-Meta-allcaps":   "capsvalue",
				"X-Amz-Meta-mixedcase": "mixed",
			},
		},
		{
			name: "special characters like dot and hash are preserved",
			headers: map[string]string{
				"X-Amz-Meta-.":        "dotvalue",
				"X-Amz-Meta-#":        "hashvalue",
				"X-Amz-Meta-Test.Key": "dotinkey",
				"X-Amz-Meta-Test#Key": "hashinkey",
			},
			expectedKeys: map[string]string{
				"X-Amz-Meta-.":        "dotvalue",
				"X-Amz-Meta-#":        "hashvalue",
				"X-Amz-Meta-test.key": "dotinkey",
				"X-Amz-Meta-test#key": "hashinkey",
			},
		},
		{
			name: "non-metadata headers are ignored",
			headers: map[string]string{
				"X-Amz-Meta-Key": "metavalue",
				"Content-Type":   "application/json",
				"Authorization":  "Bearer token",
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

			for k, v := range tt.headers {
				req.Header.Set(k, v)
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

func TestAmzMetaWriteHeaders(t *testing.T) {
	tests := []struct {
		name                string
		metadata            catalog.Metadata
		expectedHeaders     map[string]string
		expectedMissingMeta string // empty string means header should not be set
	}{
		{
			name: "all valid metadata keys",
			metadata: catalog.Metadata{
				"X-Amz-Meta-validkey":   "validvalue",
				"X-Amz-Meta-anotherkey": "anothervalue",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Meta-Validkey":   "validvalue",
				"X-Amz-Meta-Anotherkey": "anothervalue",
			},
			expectedMissingMeta: "",
		},
		{
			name: "special characters like dot and hash",
			metadata: catalog.Metadata{
				"X-Amz-Meta-.":        "dotvalue",
				"X-Amz-Meta-#":        "hashvalue",
				"X-Amz-Meta-test.key": "dotinkey",
				"X-Amz-Meta-test#key": "hashinkey",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Meta-.":        "dotvalue",
				"X-Amz-Meta-#":        "hashvalue",
				"X-Amz-Meta-Test.key": "dotinkey",
				"X-Amz-Meta-Test#key": "hashinkey",
			},
			expectedMissingMeta: "",
		},
		{
			name: "some invalid metadata keys with non-printable characters",
			metadata: catalog.Metadata{
				"X-Amz-Meta-validkey":      "validvalue",
				"X-Amz-Meta-invalid\x99":   "shouldbeskipped", // non-printable character
				"X-Amz-Meta-invalid\x7f":   "shouldbeskipped", // DEL character
				"X-Amz-Meta-unicode\u00e9": "shouldbeskipped", // unicode character (beyond ASCII)
				"X-Amz-Meta-validkey2":     "validvalue2",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Meta-Validkey":  "validvalue",
				"X-Amz-Meta-Validkey2": "validvalue2",
			},
			expectedMissingMeta: "3",
		},
		{
			name: "all invalid metadata keys",
			metadata: catalog.Metadata{
				"X-Amz-Meta-\x01key": "value1", // non-printable start
				"X-Amz-Meta-key\x02": "value2", // non-printable middle
				"X-Amz-Meta-":        "empty",  // empty key
			},
			expectedHeaders:     map[string]string{},
			expectedMissingMeta: "3",
		},
		{
			name: "non-metadata headers are ignored",
			metadata: catalog.Metadata{
				"X-Amz-Meta-valid":       "value",
				"Content-Type":           "application/json",
				"Authorization":          "Bearer token",
				"X-Amz-Meta-invalid\x03": "shouldbeskipped",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Meta-Valid": "value", // Go canonicalizes header names
			},
			expectedMissingMeta: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			amzMetaWriteHeaders(w, tt.metadata)

			headers := w.Header()

			// Check expected headers are present
			for expectedKey, expectedValue := range tt.expectedHeaders {
				actualValue := headers.Get(expectedKey)
				require.Equal(t, expectedValue, actualValue, "header %q value mismatch", expectedKey)
			}

			// Check X-Amz-Missing-Meta header
			missingMetaValue := headers.Get(amzMissingMetaHeader)
			if tt.expectedMissingMeta == "" {
				require.Empty(t, missingMetaValue, "X-Amz-Missing-Meta header should not be set")
			} else {
				require.Equal(t, tt.expectedMissingMeta, missingMetaValue, "X-Amz-Missing-Meta header value mismatch")
			}

			// Ensure no unexpected X-Amz-Meta headers are present
			for headerName := range headers {
				if strings.HasPrefix(headerName, "X-Amz-Meta-") {
					_, expected := tt.expectedHeaders[headerName]
					require.True(t, expected, "unexpected metadata header: %s", headerName)
				}
			}
		})
	}
}
