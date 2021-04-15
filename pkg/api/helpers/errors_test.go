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
	cases := []struct {
		name     string
		response interface{}
		message  string // non-empty to match return error; empty to signal no error
	}{
		{"no HTTPResponse field", &struct{ A int }{17}, "no HTTPResponse"},
		{"OK", &Response{&http.Response{StatusCode: 234}}, ""},
		{"status code", &Response{&http.Response{StatusCode: 418}}, http.StatusText(418)},
		{
			"status message",
			&Response{&http.Response{StatusCode: 418, Status: "espresso"}},
			"espresso",
		},
		{
			"non-JSON body",
			&Body{Response{&http.Response{StatusCode: 418}}, []byte("it's not JSON")},
			http.StatusText(418),
		},
		{
			"JSON body with no message",
			&Body{Response{&http.Response{StatusCode: 418}}, []byte("{\"yes\": true}")},
			http.StatusText(418),
		},
		{
			"JSON body",
			&Body{Response{&http.Response{StatusCode: 418}}, []byte("{\"message\": \"lemonade\"}")},
			"[I'm a teapot] lemonade",
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
				if err == nil || err.Error() != fmt.Errorf("%w: %s", helpers.ErrRequestFailed, tt.message).Error() {
					t.Errorf("got error \"%s\" but wanted \"%s\"", err, tt.message)
				}
			}
		})
	}
}
