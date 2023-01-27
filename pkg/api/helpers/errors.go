package helpers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/treeverse/lakefs/pkg/api"
)

var (
	// ErrUnsupportedProtocol is the error returned when lakeFS server requests the client
	// use a protocol it does not know about.  Until recompiled to use a newer client,
	// upload the object using through the lakeFS server.
	ErrUnsupportedProtocol = errors.New("unsupported protocol")

	// ErrRequestFailed is an error returned for failing lakeFS server replies.
	ErrRequestFailed = errors.New("request failed")
	ErrConflict      = errors.New("conflict")
)

const minHTTPErrorStatusCode = 400

// isOK returns true if statusCode is an OK HTTP status code: 0-399.
func isOK(statusCode int) bool {
	return statusCode < minHTTPErrorStatusCode
}

// APIFields are fields to use to format an HTTP error response that can be
// shown to the user.
type APIFields struct {
	StatusCode int
	Status     string
	Message    string
}

// CallFailedError is an error performing the HTTP request itself formatted
// to be shown to a user.  It does _not_ update its message when wrapped so
// usually should not be wrapped.
type CallFailedError struct {
	Err     error
	Message string
}

func (e CallFailedError) Error() string {
	wrapped := ""
	if e.Err != nil {
		wrapped = ": " + e.Err.Error()
	}
	return fmt.Sprintf("[%s]%s", e.Message, wrapped)
}

func (e CallFailedError) Unwrap() error {
	return e.Err
}

// UserVisibleAPIError is an HTTP error response formatted to be shown to a
// user.  It does _not_ update its message when wrapped so usually should
// not be wrapped.
type UserVisibleAPIError struct {
	APIFields
	Err error
}

// space stringifies non-nil elements from s... and returns all the
// non-empty resulting strings joined with spaces.
func spaced(s ...interface{}) string {
	ret := make([]string, 0, len(s))
	for _, t := range s {
		if t != nil {
			r := fmt.Sprint(t)
			if r != "" {
				ret = append(ret, r)
			}
		}
	}
	return strings.Join(ret, " ")
}

func (e UserVisibleAPIError) Error() string {
	message := spaced(e.Message, e.Err.Error())
	if message != "" {
		message = ": " + message
	}
	return fmt.Sprintf("[%s]%s", e.Status, message)
}

func (e UserVisibleAPIError) Unwrap() error {
	return e.Err
}

// ResponseAsError returns a UserVisibleAPIError wrapping an ErrRequestFailed
// wrapping a response from the server.  It searches for a non-nil
// unsuccessful HTTPResponse field and uses its message, along with a Body
// that it assumes is an api.Error.
func ResponseAsError(response interface{}) error {
	if response == nil {
		return nil
	}
	if httpResponse, ok := response.(*http.Response); ok {
		return HTTPResponseAsError(httpResponse)
	}
	r := reflect.Indirect(reflect.ValueOf(response))
	if !r.IsValid() || r.Kind() != reflect.Struct {
		return CallFailedError{
			Message: fmt.Sprintf("bad type %s: must reference a struct", r.Type().Name()),
			Err:     ErrRequestFailed,
		}
	}
	var ok bool
	f := r.FieldByName("HTTPResponse")
	if !f.IsValid() {
		return fmt.Errorf("[no HTTPResponse]: %w", ErrRequestFailed)
	}
	httpResponse, ok := f.Interface().(*http.Response)
	if !ok {
		return fmt.Errorf("%w: no HTTPResponse", ErrRequestFailed)
	}
	if httpResponse == nil || isOK(httpResponse.StatusCode) {
		return nil
	}

	statusCode := httpResponse.StatusCode
	statusText := httpResponse.Status
	if statusText == "" {
		statusText = http.StatusText(statusCode)
	}

	var message string
	f = r.FieldByName("Body")
	if f.IsValid() && f.Type().Kind() == reflect.Slice && f.Type().Elem().Kind() == reflect.Uint8 {
		body := f.Bytes()
		var apiError api.Error
		if json.Unmarshal(body, &apiError) == nil && apiError.Message != "" {
			message = apiError.Message
		}
	}

	return UserVisibleAPIError{
		Err: ErrRequestFailed,
		APIFields: APIFields{
			StatusCode: statusCode,
			Status:     statusText,
			Message:    message,
		},
	}
}

func HTTPResponseAsError(httpResponse *http.Response) error {
	if httpResponse == nil || isOK(httpResponse.StatusCode) {
		return nil
	}
	statusCode := httpResponse.StatusCode
	statusText := httpResponse.Status
	if statusText == "" {
		statusText = http.StatusText(statusCode)
	}
	var message string
	body, err := io.ReadAll(httpResponse.Body)
	if err == nil {
		var apiError api.Error
		if json.Unmarshal(body, &apiError) == nil && apiError.Message != "" {
			message = apiError.Message
		}
	}
	return UserVisibleAPIError{
		Err: ErrRequestFailed,
		APIFields: APIFields{
			StatusCode: statusCode,
			Status:     statusText,
			Message:    message,
		},
	}
}
