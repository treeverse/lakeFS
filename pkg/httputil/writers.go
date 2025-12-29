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

// ErrorResponse represents an error message response in our API
// we do not include the generated code for this struct to enable import code without
// dependence on the generated code
type ErrorResponse struct {
	Message string `json:"message"`
}

// WriteAPIError writes an error for a lakeFS API request
func WriteAPIError(w http.ResponseWriter, r *http.Request, code int, v any) {
	apiErr := ErrorResponse{
		Message: fmt.Sprint(v),
	}
	WriteAPIResponse(w, r, code, apiErr)
}

// WriteAPIResponse writes a response for lakeFS API request
func WriteAPIResponse(w http.ResponseWriter, r *http.Request, code int, response any) {
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
