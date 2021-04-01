package helpers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/treeverse/lakefs/pkg/api"
)

var (
	// ErrUnsupportedProtocol is the error returned when lakeFS server requests the client
	// use a protocol it does not know about.  Until recompiled to use a newer client,
	// upload the object using through the lakeFS server.
	ErrUnsupportedProtocol = errors.New("unsupported protocol")

	// ErrRequestFailed is an error returned for failing lakeFS server replies.
	ErrRequestFailed = errors.New("request failed")
)

const minHTTPErrorStatusCode = 400

// isOK returns true if statusCode is an OK HTTP status code: 0-399.
func isOK(statusCode int) bool {
	return statusCode < minHTTPErrorStatusCode
}

// ResponseAsError returns a ErrRequestFailed wrapping a response from the server.  It
// searches for a non-nil unsuccessful HTTPResponse field and uses its message, along with a
// Body that it assumes is an api.Error.
func ResponseAsError(response interface{}) error {
	r := reflect.Indirect(reflect.ValueOf(response))
	if !r.IsValid() {
		return fmt.Errorf("%w: bad type: cannot indirect", ErrRequestFailed)
	}

	f := r.FieldByName("HTTPResponse")
	if !f.IsValid() {
		return fmt.Errorf("%w: no HTTPResponse", ErrRequestFailed)
	}
	httpResponse, ok := f.Interface().(*http.Response)
	if !ok {
		return fmt.Errorf("%w: no HTTPResponse", ErrRequestFailed)
	}
	if httpResponse == nil || isOK(httpResponse.StatusCode) {
		return nil
	}

	message := http.StatusText(httpResponse.StatusCode)
	if httpResponse.Status != "" {
		message = httpResponse.Status
	}

	f = r.FieldByName("Body")
	if f.IsValid() && f.Type().Kind() == reflect.Slice && f.Type().Elem().Kind() == reflect.Uint8 {
		body := f.Bytes()
		var apiError api.Error
		if json.Unmarshal(body, &apiError) == nil && apiError.Message != "" {
			message = apiError.Message
		}
	}

	return fmt.Errorf("%w: %s", ErrRequestFailed, message)
}
