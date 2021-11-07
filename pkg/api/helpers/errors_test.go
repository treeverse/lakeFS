package helpers_test

import (
	"github.com/treeverse/lakefs/pkg/api/helpers"

	"fmt"
	"net/http"
	"testing"
)

type Response struct {
	HTTPResponse *http.Response
}

type Body struct {
	Response
	Body []byte
}

func TestResponseAsError(t *testing.T) {
	expectedClean418 := fmt.Sprintf("[%s]: %s", http.StatusText(418), "request failed")

	cases := []struct {
		name     string
		response interface{}
		message  string // non-empty to match return error; empty to signal no error
	}{
		{"no_HTTPResponse_field", &struct{ A int }{17}, "[no HTTPResponse]: request failed"},
		{"OK", &Response{&http.Response{StatusCode: 234}}, ""},
		{"status_code", &Response{&http.Response{StatusCode: 418}}, expectedClean418},
		{
			"status message",
			&Response{&http.Response{StatusCode: 418, Status: "espresso"}},
			"[espresso]: request failed",
		},
		{
			"non-JSON body",
			&Body{Response{&http.Response{StatusCode: 418}}, []byte("it's not JSON")},
			expectedClean418,
		},
		{
			"JSON body with no message",
			&Body{Response{&http.Response{StatusCode: 418}}, []byte("{\"yes\": true}")},
			expectedClean418,
		},
		{
			"JSON body",
			&Body{Response{&http.Response{StatusCode: 418}}, []byte("{\"message\": \"lemonade\"}")},
			"[I'm a teapot]: lemonade request failed",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := helpers.ResponseAsError(tt.response)
			if tt.message == "" {
				if err != nil {
					t.Errorf("unexpected error %s", err)
				}
			} else {
				if err == nil || err.Error() != tt.message {
					t.Errorf("got error \"%s\" but wanted \"%s\"", err, tt.message)
				}
			}
		})
	}
}
