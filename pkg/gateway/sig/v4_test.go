package sig_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"

	"github.com/treeverse/lakefs/pkg/auth/model"

	"github.com/treeverse/lakefs/pkg/gateway/sig"

	"github.com/treeverse/lakefs/pkg/gateway/errors"
)

const sigV4NoDomain = ""

var mockCreds = &model.Credential{
	BaseCredential: model.BaseCredential{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	},
}

func TestDoesPolicySignatureMatch(t *testing.T) {
	testCases := []struct {
		Name               string
		Header             http.Header
		Host               string
		Method             string
		Path               string
		ExpectedParseError bool
		ExpectedError      bool
	}{
		{
			Name:               "no headers",
			Header:             http.Header{},
			ExpectedParseError: true,
		},
		{
			Name: "missing headers",
			Header: http.Header{
				"Authorization": []string{"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"},
			},
			ExpectedError: true,
		},
		{
			Name: "It should fail with a bad signature.",
			Header: http.Header{
				"X-Amz-Credential": []string{"EXAMPLEINVALIDEXAMPL/20130524/us-east-1/s3/aws4_request"},
				"X-Amz-Date":       []string{"20130524T000000Z"},
				"X-Amz-Signature":  []string{"invalidsignature"},
				"Policy":           []string{"policy"},
			},
			ExpectedParseError: true,
		},
		{
			Name: "Amazon single chunk example", //  https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
			Header: http.Header{
				"X-Amz-Credential":     []string{"AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"},
				"X-Amz-Date":           []string{"20130524T000000Z"},
				"X-Amz-Content-Sha256": []string{"44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"},
				"Authorization":        []string{"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"},
				"X-Amz-Storage-Class":  []string{"REDUCED_REDUNDANCY"},
				"Policy":               []string{"policy"},

				"Date": []string{"Fri, 24 May 2013 00:00:00 GMT"},
			},
			Host:   "examplebucket.s3.amazonaws.com",
			Method: http.MethodPut,
			Path:   "test$file.text",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			url := fmt.Sprintf("http://%s/%s", tc.Host, tc.Path)
			req, e := http.NewRequest(tc.Method, url, nil)
			if e != nil {
				t.Fatalf("failed to create http.Request, got %v", e)
			}

			// Do the same for the headers.
			req.Header = tc.Header
			authenticator := sig.NewV4Authenticator(req)
			_, err := authenticator.Parse()
			if err != nil {
				if !tc.ExpectedParseError {
					t.Fatal(err)
				}
				return
			}

			err = authenticator.Verify(mockCreds, sigV4NoDomain)
			if err != nil {
				if !tc.ExpectedError {
					t.Fatal(err)
				}
				return
			}
		})
	}
}

func TestSingleChunkPut(t *testing.T) {
	tt := []struct {
		Name              string
		Host              string
		RequestBody       string
		SignBody          string
		ExpectedReadError error
	}{
		{
			Name:        "amazon example",
			RequestBody: "Welcome to Amazon S3.",
			SignBody:    "Welcome to Amazon S3.",
		},
		{
			Name:              "amazon example should fail",
			RequestBody:       "Welcome to Amazon S3",
			SignBody:          "Welcome to Amazon S3.",
			ExpectedReadError: errors.ErrSignatureDoesNotMatch,
		},
		{
			Name:        "empty body",
			RequestBody: "",
			SignBody:    "",
		},
	}
	const (
		PATH   = "http://example.test/foo"
		ID     = "AKIAIOSFODNN7EXAMPLE"
		SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			// build request with amazons sdk
			creds := credentials.NewStaticCredentials(ID, SECRET, "")
			signer := v4.NewSigner(creds)

			req, err := http.NewRequest(http.MethodPut, PATH, nil)
			if err != nil {
				t.Errorf("expect not no error, got %v", err)
			}

			_, err = signer.Sign(req, strings.NewReader(tc.SignBody), "s3", "us-east-1", time.Now())
			if err != nil {
				t.Errorf("expect not no error, got %v", err)
			}

			req.Body = io.NopCloser(strings.NewReader(tc.RequestBody))
			// verify request with our authenticator

			authenticator := sig.NewV4Authenticator(req)
			_, err = authenticator.Parse()
			if err != nil {
				t.Errorf("expect not no error, got %v", err)
			}

			err = authenticator.Verify(&model.Credential{
				BaseCredential: model.BaseCredential{
					AccessKeyID:     ID,
					SecretAccessKey: SECRET,
					IssuedDate:      time.Now(),
				},
			}, sigV4NoDomain)
			if err != nil {
				t.Errorf("expect not no error, got %v", err)
			}

			// read all
			_, err = io.ReadAll(req.Body)
			if err != tc.ExpectedReadError {
				t.Errorf("expect Error %v error, got %s", tc.ExpectedReadError, err)
			}
		})
	}
}

func TestStreaming(t *testing.T) {
	const (
		method = http.MethodPut
		host   = "s3.amazonaws.com"
		path   = "examplebucket/chunkObject.txt"
		ID     = "AKIAIOSFODNN7EXAMPLE"
		SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	req, err := http.NewRequest(method, fmt.Sprintf("https://%s/%s", host, path), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = http.Header{
		"Authorization":                []string{"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"},
		"X-Amz-Credential":             []string{"EXAMPLEINVALIDEXAMPL/20130524/us-east-1/s3/aws4_request"},
		"X-Amz-Date":                   []string{"20130524T000000Z"},
		"X-Amz-Storage-Class":          []string{"REDUCED_REDUNDANCY"},
		"X-Amz-Content-Sha256":         []string{"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
		"Content-Encoding":             []string{"aws-chunked"},
		"X-Amz-Decoded-Content-Length": []string{"66560"},
		"Content-Length":               []string{"66824"},
	}
	chunk1Size := 65536
	a := bytes.Repeat([]byte("a"), chunk1Size)
	a = append(a, '\r', '\n')
	chunk1 := append([]byte("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"), a...)
	chunk2Size := 1024
	b := bytes.Repeat([]byte("a"), chunk2Size)
	b = append(b, '\r', '\n')
	chunk2 := append([]byte("400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n"), b...)
	chunk3 := []byte("0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n")
	body := append(chunk1, chunk2...)
	body = append(body, chunk3...)
	req.Body = io.NopCloser(bytes.NewReader(body))

	// now test it
	authenticator := sig.NewV4Authenticator(req)
	_, err = authenticator.Parse()
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}

	err = authenticator.Verify(&model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:     ID,
			SecretAccessKey: SECRET,
			IssuedDate:      time.Now(),
		},
	}, sigV4NoDomain)
	if err != nil {
		t.Error(err)
	}
	if req.ContentLength != int64(chunk1Size+chunk2Size) {
		t.Fatal("expected content length to be equal to decoded content length")
	}
	_, err = io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamingLastByteWrong(t *testing.T) {
	const (
		method = http.MethodPut
		ID     = "AKIAIOSFODNN7EXAMPLE"
		SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	req, err := http.NewRequest(method, "https://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = http.Header{
		"Authorization":                []string{"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"},
		"X-Amz-Credential":             []string{"EXAMPLEINVALIDEXAMPL/20130524/us-east-1/s3/aws4_request"},
		"X-Amz-Date":                   []string{"20130524T000000Z"},
		"X-Amz-Storage-Class":          []string{"REDUCED_REDUNDANCY"},
		"X-Amz-Content-Sha256":         []string{"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
		"Content-Encoding":             []string{"aws-chunked"},
		"X-Amz-Decoded-Content-Length": []string{"66560"},
		"Content-Length":               []string{"66824"},
	}

	a := bytes.Repeat([]byte("a"), 65536)
	a = append(a, '\r', '\n')
	chunk1 := append([]byte("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"), a...)
	b := bytes.Repeat([]byte("a"), 1023)
	b = append(b, 'b', '\r', '\n')
	chunk2 := append([]byte("400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n"), b...)
	chunk3 := []byte("0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n")
	body := append(chunk1, chunk2...)
	body = append(body, chunk3...)
	req.Body = io.NopCloser(bytes.NewReader(body))

	// now test it
	authenticator := sig.NewV4Authenticator(req)
	_, err = authenticator.Parse()
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}

	err = authenticator.Verify(&model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:     ID,
			SecretAccessKey: SECRET,
			IssuedDate:      time.Now(),
		},
	}, sigV4NoDomain)
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}

	_, err = io.ReadAll(req.Body)
	if err != errors.ErrSignatureDoesNotMatch {
		t.Errorf("expect %v, got %v", errors.ErrSignatureDoesNotMatch, err)
	}
}

func TestUnsignedPayload(t *testing.T) {
	const (
		testID     = "AKIAIOSFODNN7EXAMPLE"
		testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	req, err := http.NewRequest(http.MethodHead, "https://repo1.s3.dev.lakefs.io/imdb-spark/collections/shows/title.basics.tsv.gz", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = http.Header{
		"X-Forwarded-For":       []string{"10.20.1.90"},
		"X-Forwarded-Proto":     []string{"https"},
		"X-Forwarded-Port":      []string{"443"},
		"Host":                  []string{"repo1.s3.dev.lakefs.io"},
		"X-Amzn-Trace-UploadId": []string{"Root=1-5eb036bc-dd84b3a2115db68a77b1c068"},
		"amz-sdk-invocation-id": []string{"a8288d69-e8fa-219d-856b-b58b53b6fd5b"},
		"amz-sdk-retry":         []string{"0/0/500"},
		"Authorization":         []string{"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200504/dev/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;content-type;host;user-agent;x-amz-content-sha256;x-amz-date, Signature=9e54ee9b3917a632abc594f4a013cd0580331e627f60de9fffac26ba5b067b81"},
		"Content-Type":          []string{"application/octet-stream"},
		"User-Agent":            []string{"Hadoop 2.8.5-amzn-5, aws-sdk-java/1.11.682 Linux/4.14.154-99.181.amzn1.x86_64 OpenJDK_64-Bit_Server_VM/25.242-b08 java/1.8.0_242 scala/2.11.12 vendor/Oracle_Corporation"},
		"x-amz-content-sha256":  []string{"UNSIGNED-PAYLOAD"},
		"X-Amz-Date":            []string{"20200504T153732Z"},
	}

	authenticator := sig.NewV4Authenticator(req)
	_, err = authenticator.Parse()
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}

	err = authenticator.Verify(&model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:     testID,
			SecretAccessKey: testSecret,
			IssuedDate:      time.Now(),
		},
	}, sigV4NoDomain)
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}
}
