package httputil

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// HttpStatusClientClosedRequest used as internal status code when request context is cancelled
	HttpStatusClientClosedRequest = 499
	// HttpStatusClientClosedRequestText text used for client closed request status code
	HttpStatusClientClosedRequestText = "Client closed request"
)

// IsSuccessStatusCode returns true for status code 2xx
func IsSuccessStatusCode(response *http.Response) bool {
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}

// WriteError writes an error response with the given status code and error message
func WriteError(w http.ResponseWriter, r *http.Request, code int, v interface{}) {
	apiErr := struct {
		Message string `json:"message"`
	}{
		Message: fmt.Sprint(v),
	}
	WriteResponse(w, r, code, apiErr)
}

// WriteResponse writes a JSON response with the given status code and response body
func WriteResponse(w http.ResponseWriter, r *http.Request, code int, response interface{}) {
	// check first if the client canceled the request
	if IsRequestCanceled(r) {
		w.WriteHeader(HttpStatusClientClosedRequest) // Client closed request
		return
	}
	// nobody - just status code
	if response == nil {
		w.WriteHeader(code)
		return
	}
	// encode response body as json
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		logging.FromContext(r.Context()).WithError(err).WithField("code", code).Info("Failed to write encoded json response")
	}
}
