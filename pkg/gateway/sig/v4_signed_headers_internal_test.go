package sig

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
)

func TestVerifySignedHeaders(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		signedHeaders []string
		header        http.Header
		expectedErr   error
	}{
		{
			name:          "accepts x-amz headers that are signed",
			signedHeaders: []string{"host", "x-amz-copy-source"},
			header:        http.Header{"X-Amz-Copy-Source": {"/src/main/a.txt"}},
		},
		{
			name:          "accepts unsigned headers that are not x-amz headers",
			signedHeaders: []string{"host"},
			header:        http.Header{"User-Agent": {"test-client"}, "Content-Type": {"text/plain"}},
		},
		{
			name:          "accepts an unsigned payload hash header",
			signedHeaders: []string{"host"},
			header:        http.Header{"X-Amz-Content-Sha256": {"UNSIGNED-PAYLOAD"}},
		},
		{
			name:          "rejects unsigned x-amz headers and lists them",
			signedHeaders: []string{"host"},
			header:        http.Header{"X-Amz-Storage-Class": {"GLACIER"}, "X-Amz-Copy-Source": {"/src/main/a.txt"}, "X-Amz-Meta-Owner": {"eve"}},
			expectedErr:   &gatewayerrors.UnsignedHeadersError{Headers: []string{"x-amz-copy-source", "x-amz-meta-owner", "x-amz-storage-class"}},
		},
		{
			name:          "rejects a request that does not sign host",
			signedHeaders: []string{"x-amz-date"},
			header:        http.Header{"X-Amz-Date": {"20260923T000000Z"}},
			expectedErr:   &gatewayerrors.UnsignedHeadersError{Headers: []string{"host"}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &verificationCtx{
				Request:   &http.Request{Header: tc.header},
				AuthValue: V4Auth{SignedHeaders: tc.signedHeaders},
			}

			require.Equal(t, tc.expectedErr, ctx.verifySignedHeaders())
		})
	}
}
