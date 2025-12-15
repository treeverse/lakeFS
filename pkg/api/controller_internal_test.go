package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// TestHandleApiErrorCallback_PredicateFailed tests that both kv.ErrPredicateFailed
// and graveler.ErrPreconditionFailed are properly mapped to HTTP 412 Precondition Failed.
// This is a regression test for the bug where kv.ErrPredicateFailed was falling through
// to the default case and returning 500 Internal Server Error instead of 412.
func TestHandleApiErrorCallback_PredicateFailed(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "graveler.ErrPreconditionFailed returns 412",
			err:            graveler.ErrPreconditionFailed,
			expectedStatus: http.StatusPreconditionFailed,
			expectedBody:   "Precondition failed",
		},
		{
			name:           "kv.ErrPredicateFailed returns 412",
			err:            kv.ErrPredicateFailed,
			expectedStatus: http.StatusPreconditionFailed,
			expectedBody:   "Precondition failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test HTTP response recorder
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/api/v1/repositories/test/branches/main/objects?path=test", nil)

			// Create a logger
			logger := logging.Dummy()

			// Test the internal error handling function
			handled := handleApiErrorCallback(logger, w, r, tt.err, func(w http.ResponseWriter, r *http.Request, code int, v interface{}) {
				w.WriteHeader(code)
				if msg, ok := v.(string); ok {
					_, _ = w.Write([]byte(msg))
				} else if err, ok := v.(error); ok {
					_, _ = w.Write([]byte(err.Error()))
				}
			})

			// Verify the error was handled
			require.True(t, handled, "error should be handled")

			// Verify the status code
			require.Equal(t, tt.expectedStatus, w.Code, "unexpected status code")

			// Verify the response body contains expected message
			body := w.Body.String()
			require.Contains(t, body, tt.expectedBody, "response body should contain expected message")
		})
	}
}

// TestHandleApiErrorCallback_NilError verifies that nil errors are not handled
func TestHandleApiErrorCallback_NilError(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	logger := logging.Dummy()

	handled := handleApiErrorCallback(logger, w, r, nil, func(w http.ResponseWriter, r *http.Request, code int, v interface{}) {
		w.WriteHeader(code)
	})

	require.False(t, handled, "nil error should not be handled")
	require.Equal(t, http.StatusOK, w.Code, "status code should not be set for nil error")
}
