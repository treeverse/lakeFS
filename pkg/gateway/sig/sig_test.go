package sig_test

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/treeverse/lakefs/pkg/auth/model"
	gwErrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/testutil"
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
	t.Parallel()
	cases := []struct {
		Name    string
		Want    bool
		Headers map[string]string
		Query   map[string]string
	}{
		{Name: "no sig", Want: false},
		{Name: "non aws auth header", Want: false, Headers: map[string]string{"Authorization": "Basic dXNlcjpwYXNzd29yZA=="}},
		{Name: "v2 auth header", Want: true, Headers: map[string]string{"Authorization": "AWS wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}},
		{Name: "v2 auth query param", Want: true, Query: map[string]string{"AWSAccessKeyId": "bPxRfiCYEXAMPLEKEY"}},
		{Name: "v4 auth header", Want: true, Headers: map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"}},
		{Name: "v4 auth query param", Want: true, Query: map[string]string{"X-Amz-Credential": "bPxRfiCYEXAMPLEKEY"}},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			r := makeRequest(t, tc.Headers, tc.Query)
			got := sig.IsAWSSignedRequest(r)
			if got != tc.Want {
				t.Fatalf("IsAWSSignedRequest with %s: got %v, expected %v", tc.Name, got, tc.Want)
			}
		})
	}
}

type (
	Signer   func(req http.Request) *http.Request
	Verifier func(req *http.Request) error
)

type Style string

func MakeV2Signer(keyID, secretKey string, style Style) Signer {
	return func(req http.Request) *http.Request {
		return signer.SignV2(req, keyID, secretKey, style == "host")
	}
}

func MakeV4Signer(keyID, secretKey, location string) Signer {
	return func(req http.Request) *http.Request {
		return signer.SignV4(req, keyID, secretKey, "", location)
	}
}

func MakeV2Verifier(keyID, secretKey, bareDomain string) Verifier {
	return func(req *http.Request) error {
		authenticator := sig.NewV2SigAuthenticator(req)
		_, err := authenticator.Parse()
		if err != nil {
			return fmt.Errorf("sigV2 parse failed: %w", err)
		}
		return authenticator.Verify(
			&model.Credential{BaseCredential: model.BaseCredential{AccessKeyID: keyID, SecretAccessKey: secretKey}},
			bareDomain,
		)
	}
}

func MakeV4Verifier(keyID, secretKey, bareDomain string) Verifier {
	return func(req *http.Request) error {
		authenticator := sig.NewV4Authenticator(req)
		_, err := authenticator.Parse()
		if err != nil {
			return fmt.Errorf("sigV4 parse failed: %w", err)
		}
		return authenticator.Verify(
			&model.Credential{BaseCredential: model.BaseCredential{AccessKeyID: keyID, SecretAccessKey: secretKey}},
			bareDomain,
		)
	}
}

func MakeHeader(m map[string]string) http.Header {
	ret := http.Header{}
	for k, v := range m {
		ret.Add(k, v)
	}
	return ret
}

const (
	keyID     = "AKIAIOSFODNN7EXAMPLE"
	secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	domain    = "s3.lakefs.test"
	location  = "lu-alpha-1"
)

var date = time.Unix(1631523198, 0)

type SignCase struct {
	Name     string
	Signer   Signer
	Verifier Verifier
	Style    Style
}

var signatures = []SignCase{
	{
		Name:     "V2Host",
		Signer:   MakeV2Signer(keyID, secretKey, "host"),
		Verifier: MakeV2Verifier(keyID, secretKey, domain),
		Style:    "host",
	}, {
		Name:     "V2Path",
		Signer:   MakeV2Signer(keyID, secretKey, "path"),
		Verifier: MakeV2Verifier(keyID, secretKey, domain),
		Style:    "path",
	}, {
		Name:     "V4Host",
		Signer:   MakeV4Signer(keyID, secretKey, location),
		Verifier: MakeV4Verifier(keyID, secretKey, domain),
		Style:    "host",
	}, {
		Name:     "V4Path",
		Signer:   MakeV4Signer(keyID, secretKey, location),
		Verifier: MakeV4Verifier(keyID, secretKey, domain),
		Style:    "path",
	},
}

func TestAWSSigVerify(t *testing.T) {
	t.Parallel()
	const (
		numRounds  = 100
		seed       = 20210913
		pathLength = 900
		bucket     = "my-bucket"
	)
	methods := []string{"GET", "PUT", "DELETE", "PATCH"}

	for _, s := range signatures {
		t.Run("Sig"+s.Name, func(t *testing.T) {
			host := domain
			if s.Style == "host" {
				host = fmt.Sprintf("%s.%s", bucket, domain)
			}

			r := rand.New(rand.NewSource(seed))
			for i := 0; i < numRounds; i++ {
				path := s3utils.EncodePath("my-branch/ariels/x/" + testutil.RandomString(r, pathLength))
				bucketURL := &url.URL{
					Scheme: "s3",
					Host:   bucket,
					Path:   path,
					// No way to construct possibly-equivalent forms, so let
					// URL construct The Right RawPath.
					RawPath: "",
				}
				req := http.Request{
					Method: methods[r.Intn(len(methods))],
					Host:   host,
					URL:    bucketURL,
					Header: MakeHeader(map[string]string{
						"Date":         date.Format(http.TimeFormat),
						"x-amz-date":   date.Format("20060102T150405Z"),
						"Content-Md5":  "deadbeef",
						"Content-Type": "application/binary",
					}),
				}
				signedReq := s.Signer(req)
				err := s.Verifier(signedReq)
				if err != nil {
					errText := err.Error()
					var apiErr gwErrors.APIErrorCode
					if errors.As(err, &apiErr) {
						errText = apiErr.ToAPIErr().Description
					}
					t.Errorf("Sign and verify %s: %s", bucketURL.String(), errText)
				}
			}
		})
	}
}
