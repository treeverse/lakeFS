package helpers

import (
	"net/http"
)

// executeHTTPRequest executes an HTTP request and ensures the response body is closed with defer.
// The callback function receives the response and should return an error if the request should be retried.
// The response body will be automatically closed after the callback returns.
func executeHTTPRequest(client *http.Client, req *http.Request, callback func(*http.Response) error) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return callback(resp)
}
