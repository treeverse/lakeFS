package sig_test

import (
	"net/http"
	"testing"

	"github.com/treeverse/lakefs/pkg/gateway/sig"
)

func makeRequest(t *testing.T, headers map[string]string, query map[string]string) *http.Request {
	r, err := http.NewRequest("GET", "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	q := r.URL.Query()
	for k, v := range query {
		q.Add(k, v)
	}
	r.URL.RawQuery = q.Encode()
	return r
}

func TestIsAWSSignedRequest(t *testing.T) {
	type KV map[string]string
	cases := []struct {
		name    string
		want    bool
		headers map[string]string
		query   map[string]string
	}{
		{"no sig", false, nil, nil},
		{"non aws auth header", false, KV{"Authorization": "Basic dXNlcjpwYXNzd29yZA=="}, nil},
		{"v2 auth header", true, KV{"Authorization": "AWS wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}, nil},
		{"v2 auth query param", true, nil, KV{"AWSAccessKeyId": "bPxRfiCYEXAMPLEKEY"}},
		{"v4 auth header", true, KV{"Authorization": "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"}, nil},
		{"v4 auth query param", true, nil, KV{"X-Amz-Credential": "bPxRfiCYEXAMPLEKEY"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := makeRequest(t, tc.headers, tc.query)
			got := sig.IsAWSSignedRequest(r)
			if got != tc.want {
				t.Fatalf("IsAWSSignedRequest with %s: got %v, expected %v", tc.name, got, tc.want)
			}
		})
	}
}
