package httputil

import "net/http"

// IsSuccessStatusCode returns true for status code 2xx
func IsSuccessStatusCode(response *http.Response) bool {
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}
