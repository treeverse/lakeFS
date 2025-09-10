package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/params"
)

// TestSecurityMiddlewareIntegration tests that the security middleware is properly set on the UI handler
func TestSecurityMiddlewareIntegration(t *testing.T) {
	handler := api.NewUIHandler([]string{}, []params.CodeSnippet{})

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Check that security headers are set
	xFrameOptions := rec.Header().Get("X-Frame-Options")
	if xFrameOptions != "SAMEORIGIN" {
		t.Errorf("Expected X-Frame-Options header to be 'SAMEORIGIN', got '%s'", xFrameOptions)
	}

	xContentTypeOptions := rec.Header().Get("X-Content-Type-Options")
	if xContentTypeOptions != "nosniff" {
		t.Errorf("Expected X-Content-Type-Options header to be 'nosniff', got '%s'", xContentTypeOptions)
	}
}
