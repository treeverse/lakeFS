package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

// countingReader records how many bytes were read through it.
type countingReader struct {
	io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n += n
	return n, err
}

func newValidatorRouter(t *testing.T) (*openapi3filter.Options, func(*http.Request) (int, error)) {
	t.Helper()
	swagger, err := apigen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	options := &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}
	return options, func(r *http.Request) (int, error) {
		route, params, err := router.FindRoute(r)
		if err != nil {
			t.Fatalf("find route: %v", err)
		}
		return validateRequest(r, route, params, options)
	}
}

// The validator must never consume an upload body: uploadObject is marked
// x-validation-exclude-body, and the handler streams the body to the block store.
// kin-openapi >= 0.132 reads the whole body in ValidateSecurityRequirements before
// authentication unless the validator is handed a body-less request (#9409).
func TestValidateRequest_UploadBodyNotReadByValidator(t *testing.T) {
	_, validate := newValidatorRouter(t)
	const size = 4 << 20
	body := &countingReader{Reader: bytes.NewReader(bytes.Repeat([]byte("x"), size))}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/repositories/repo/branches/main/objects?path=obj", body)
	req.Header.Set("Content-Type", "application/octet-stream")

	status, err := validate(req)
	if err != nil || status != http.StatusOK {
		t.Fatalf("validateRequest: status=%d err=%v", status, err)
	}
	if body.n != 0 {
		t.Fatalf("validator read %d bytes of the upload body; it must read none", body.n)
	}
	// the handler must still receive the original body in full
	got, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read original body: %v", err)
	}
	if len(got) != size {
		t.Fatalf("original body: got %d bytes, want %d", len(got), size)
	}
}

// JSON request bodies are still validated.
func TestValidateRequest_JSONBodyStillValidated(t *testing.T) {
	_, validate := newValidatorRouter(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/repositories", strings.NewReader(`{"name":123}`))
	req.Header.Set("Content-Type", "application/json")

	status, err := validate(req)
	if err == nil {
		t.Fatal("expected a validation error for a malformed body")
	}
	if status != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", status, http.StatusBadRequest)
	}
}
