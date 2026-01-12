package operations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/pkg/catalog"
	gwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func TestOperation_EncodeError_ContextCancelled(t *testing.T) {
	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()

	op := &Operation{
		Region: "us-east-1",
	}

	// Call EncodeError with a cancelled context
	op.EncodeError(rr, req, nil, gwerrors.ErrInternalError.ToAPIErr())

	// Verify that the status code is 499
	assert.Equal(t, httputil.HttpStatusClientClosedRequest, rr.Code, "Expected status code 499 for cancelled context")
}

func TestRepoOperation_EncodeError_ContextCancelled(t *testing.T) {
	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()

	op := &RepoOperation{
		AuthorizedOperation: &AuthorizedOperation{
			Operation: &Operation{
				Region: "us-east-1",
			},
			Principal: "test-user",
		},
		Repository: &catalog.Repository{
			Name: "test-repo",
		},
	}

	// Call EncodeError with a cancelled context
	op.EncodeError(rr, req, nil, gwerrors.ErrInternalError.ToAPIErr())

	// Verify that the status code is 499
	assert.Equal(t, httputil.HttpStatusClientClosedRequest, rr.Code, "Expected status code 499 for cancelled context")
}

func TestPathOperation_EncodeError_ContextCancelled(t *testing.T) {
	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()

	op := &PathOperation{
		RefOperation: &RefOperation{
			RepoOperation: &RepoOperation{
				AuthorizedOperation: &AuthorizedOperation{
					Operation: &Operation{
						Region: "us-east-1",
					},
					Principal: "test-user",
				},
				Repository: &catalog.Repository{
					Name: "test-repo",
				},
			},
			Reference: "main",
		},
		Path: "/path/to/file",
	}

	// Call EncodeError with a cancelled context
	op.EncodeError(rr, req, nil, gwerrors.ErrInternalError.ToAPIErr())

	// Verify that the status code is 499
	assert.Equal(t, httputil.HttpStatusClientClosedRequest, rr.Code, "Expected status code 499 for cancelled context")
}

func TestOperation_EncodeError_NormalError(t *testing.T) {
	// Create a normal (non-cancelled) context
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	op := &Operation{
		Region: "us-east-1",
	}

	// Call EncodeError with a normal context
	op.EncodeError(rr, req, nil, gwerrors.ErrInternalError.ToAPIErr())

	// Verify that the status code is 500 (InternalError)
	assert.Equal(t, http.StatusInternalServerError, rr.Code, "Expected status code 500 for internal error")
}

func TestOperation_EncodeResponse_ContextCancelled(t *testing.T) {
	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()

	op := &Operation{
		Region: "us-east-1",
	}

	// Call EncodeResponse with a cancelled context
	op.EncodeResponse(rr, req, map[string]string{"test": "value"}, http.StatusOK)

	// Verify that the status code is 499
	assert.Equal(t, httputil.HttpStatusClientClosedRequest, rr.Code, "Expected status code 499 for cancelled context")
}

func TestOperation_EncodeResponse_NormalResponse(t *testing.T) {
	// Create a normal (non-cancelled) context
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	op := &Operation{
		Region: "us-east-1",
	}

	type TestResponse struct {
		Value string `xml:"Value"`
	}

	// Call EncodeResponse with a normal context
	op.EncodeResponse(rr, req, TestResponse{Value: "test"}, http.StatusOK)

	// Verify that the status code is 200
	assert.Equal(t, http.StatusOK, rr.Code, "Expected status code 200 for normal response")
}

func TestAmzMetaAsMetadata(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/test", nil)
	req.Header.Set("X-Amz-Meta-Key1", "Value1")
	req.Header.Set("X-Amz-Meta-Key2", "Value2")

	metadata, err := amzMetaAsMetadata(req)

	assert.NoError(t, err)
	assert.Equal(t, "Value1", metadata["X-Amz-Meta-Key1"])
	assert.Equal(t, "Value2", metadata["X-Amz-Meta-Key2"])
}

func TestAmzMetaAsMetadata_ExactLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/test", nil)
	// Create metadata that is exactly at the 2KB limit
	// User key is "Key" (3 bytes), so we need value of 2048 - 3 = 2045 bytes
	valueSize := maxUserMetadataSize - len("Key")
	value := strings.Repeat("a", valueSize)
	req.Header.Set("X-Amz-Meta-Key", value)

	metadata, err := amzMetaAsMetadata(req)

	assert.NoError(t, err)
	assert.Equal(t, value, metadata["X-Amz-Meta-Key"])
}

func TestAmzMetaAsMetadata_ExceedingLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/test", nil)
	valueSize := maxUserMetadataSize - len("Key") + 1
	req.Header.Set("X-Amz-Meta-Key", strings.Repeat("a", valueSize))

	metadata, err := amzMetaAsMetadata(req)

	assert.ErrorIs(t, err, gwerrors.ErrMetadataTooLarge)
	assert.Nil(t, metadata)
}

func TestAmzMetaAsMetadata_MultipleHeadersExceedingLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/test", nil)
	value := strings.Repeat("a", 1000)
	req.Header.Set("X-Amz-Meta-Key1", value)
	req.Header.Set("X-Amz-Meta-Key2", value)
	req.Header.Set("X-Amz-Meta-Key3", value)

	metadata, err := amzMetaAsMetadata(req)

	assert.ErrorIs(t, err, gwerrors.ErrMetadataTooLarge)
	assert.Nil(t, metadata)
}
