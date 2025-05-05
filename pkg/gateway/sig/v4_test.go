package sig_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/treeverse/lakefs/pkg/auth/model"
	gtwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
)

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

			err = authenticator.Verify(mockCreds)
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
			ExpectedReadError: gtwerrors.ErrSignatureDoesNotMatch,
		},
		{
			Name:        "empty body",
			RequestBody: "",
			SignBody:    "",
		},
	}
	const (
		testURL             = "https://example.test/foo"
		testAccessKeyID     = "AKIAIOSFODNN7EXAMPLE"
		testSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	ctx := context.Background()
	creds := aws.Credentials{
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecretAccessKey,
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			// build request with amazons sdk
			req, err := http.NewRequest(http.MethodPut, testURL, nil)
			if err != nil {
				t.Fatalf("expect not no error, got %v", err)
			}

			h := sha256.Sum256([]byte(tc.SignBody))
			payloadHash := hex.EncodeToString(h[:])
			req.Header.Set("X-Amz-Content-Sha256", payloadHash)

			sigTime := time.Now()
			signer := v4.NewSigner()
			err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", "us-east-1", sigTime)
			if err != nil {
				t.Fatalf("expect not no error, got %v", err)
			}

			// verify request with our authenticator
			req.Body = io.NopCloser(strings.NewReader(tc.RequestBody))
			authenticator := sig.NewV4Authenticator(req)
			_, err = authenticator.Parse()
			if err != nil {
				t.Fatalf("expect not no error, got %v", err)
			}

			err = authenticator.Verify(&model.Credential{
				BaseCredential: model.BaseCredential{
					AccessKeyID:     testAccessKeyID,
					SecretAccessKey: testSecretAccessKey,
					IssuedDate:      sigTime,
				},
			})
			if err != nil {
				t.Fatalf("expect not no error, got %v", err)
			}

			// read all
			_, err = io.ReadAll(req.Body)
			if !errors.Is(err, tc.ExpectedReadError) {
				t.Errorf("expect Error %v error, got %s", tc.ExpectedReadError, err)
			}
		})
	}
}

func TestStreaming(t *testing.T) {
	const (
		host   = "s3.amazonaws.com"
		path   = "examplebucket/chunkObject.txt"
		ID     = "AKIAIOSFODNN7EXAMPLE"
		SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/%s", host, path), nil)
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
	var body bytes.Buffer

	// chunk1
	body.Write([]byte("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"))
	const chunk1Size = 65536
	body.Write(bytes.Repeat([]byte("a"), chunk1Size))
	body.Write([]byte("\r\n"))

	// chunk2
	body.Write([]byte("400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n"))
	const chunk2Size = 1024
	body.Write(bytes.Repeat([]byte("a"), chunk2Size))
	body.Write([]byte("\r\n"))

	// chunk3
	body.Write([]byte("0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n"))

	req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

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
	})
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
		key    = "AKIAIOSFODNN7EXAMPLE"
		secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	req, err := http.NewRequest(http.MethodPut, "https://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
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
			AccessKeyID:     key,
			SecretAccessKey: secret,
			IssuedDate:      time.Now(),
		},
	})
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}

	_, err = io.ReadAll(req.Body)
	if !errors.Is(err, gtwerrors.ErrSignatureDoesNotMatch) {
		t.Errorf("expect %v, got %v", gtwerrors.ErrSignatureDoesNotMatch, err)
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
	})
	if err != nil {
		t.Errorf("expect not no error, got %v", err)
	}
}

// Helper function to truncate long strings for display in error messages
func truncateForDisplay(s string) string {
	if len(s) > 40 {
		return s[:20] + "..." + s[len(s)-20:]
	}
	return s
}

func TestStreamingUnsignedPayloadTrailerWithChunks(t *testing.T) {
	testCases := []struct {
		name           string
		method         string
		host           string
		path           string
		trailerHeader  sig.ChecksumAlgorithm
		trailerContent string
		chunkSize      int
		totalChunks    int
		chunkData      [][]byte
		expectedError  bool // If true, we expect an error when using this trailer
	}{
		{
			name:           "CRC32C Trailer",
			method:         http.MethodPut,
			host:           "s3.amazonaws.com",
			path:           "examplebucket/trailertest.txt",
			trailerHeader:  sig.ChecksumAlgorithmCRC32C,
			trailerContent: "x-amz-checksum-crc32c:%s\r\n\r\n", // %s will be replaced with actual checksum
			chunkSize:      64,
			totalChunks:    2,
			chunkData: [][]byte{
				bytes.Repeat([]byte("a"), 64),
				bytes.Repeat([]byte("b"), 64),
			},
			expectedError: false,
		},
		{
			name:           "CRC32 Trailer",
			method:         http.MethodPut,
			host:           "s3.amazonaws.com",
			path:           "examplebucket/trailertest.txt",
			trailerHeader:  sig.ChecksumAlgorithmCRC32,
			trailerContent: "x-amz-checksum-crc32:%s\r\n\r\n",
			chunkSize:      64,
			totalChunks:    2,
			chunkData: [][]byte{
				bytes.Repeat([]byte("a"), 64),
				bytes.Repeat([]byte("b"), 64),
			},
			expectedError: false,
		},
		{
			name:           "Invalid Trailer",
			method:         http.MethodPut,
			host:           "s3.amazonaws.com",
			path:           "examplebucket/trailertest.txt",
			trailerHeader:  sig.ChecksumAlgorithm("invalid"),
			trailerContent: "invalid:%s\r\n\r\n",
			chunkSize:      64,
			totalChunks:    1,
			chunkData: [][]byte{
				bytes.Repeat([]byte("a"), 64),
			},
			expectedError: true,
		},
	}

	const (
		testID     = "AKIAIOSFODNN7EXAMPLE"
		testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region     = "us-east-1"
		service    = "s3"
		date       = "20130524"
		timeStamp  = "20130524T000000Z"
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a request
			req, err := http.NewRequest(tc.method, fmt.Sprintf("https://%s/%s", tc.host, tc.path), nil)
			if err != nil {
				t.Fatal(err)
			}

			// Set required headers for unsigned trailers
			req.Header.Set("X-Amz-Date", timeStamp)
			req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			req.Header.Set("Content-Encoding", "aws-chunked")
			req.Header.Set("X-Amz-Trailer", string(tc.trailerHeader))

			// Calculate decoded content length
			decodedContentLength := 0
			for _, chunk := range tc.chunkData {
				decodedContentLength += len(chunk)
			}
			req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprintf("%d", decodedContentLength))
			req.Header.Set("Host", tc.host)

			// Create AWS credentials
			creds := aws.Credentials{
				AccessKeyID:     testID,
				SecretAccessKey: testSecret,
			}

			// Sign the request using AWS SDK v4 signer
			signer := v4.NewSigner()
			err = signer.SignHTTP(context.Background(), creds, req, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", service, region, time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("Failed to sign request: %v", err)
			}

			// Prepare chunks
			var chunks strings.Builder
			var combinedData []byte

			// Create each chunk
			for _, chunkData := range tc.chunkData {
				chunkHex := fmt.Sprintf("%x\r\n", len(chunkData))
				chunks.WriteString(chunkHex)
				chunks.Write(chunkData)
				chunks.WriteString("\r\n")
				combinedData = append(combinedData, chunkData...)
			}

			// For valid trailer types, calculate checksum
			var checksumB64 string
			if !tc.expectedError {
				// Get the appropriate checksum writer using the public API
				checksumWriter, err := sig.GetChecksumWriter(string(tc.trailerHeader))
				if err != nil {
					t.Fatalf("Failed to get checksum writer for %s", tc.trailerHeader)
				}

				// Calculate checksum for the entire payload
				checksumWriter.Write(combinedData)
				checksum := checksumWriter.Sum(nil)
				checksumB64 = base64.StdEncoding.EncodeToString(checksum)
			} else {
				// For invalid trailers, just use a dummy value
				checksumB64 = "dummyvalue=="
			}

			// Add terminating chunk with trailer
			chunks.WriteString("0\r\n")
			chunks.WriteString(fmt.Sprintf(tc.trailerContent, checksumB64))

			// Set the request body
			fullBody := chunks.String()
			req.Body = io.NopCloser(strings.NewReader(fullBody))
			req.ContentLength = int64(len(fullBody))

			// Create credential for verification
			modelCred := model.Credential{
				BaseCredential: model.BaseCredential{
					AccessKeyID:     testID,
					SecretAccessKey: testSecret,
					IssuedDate:      time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC),
				},
			}

			// Parse the request using our authenticator
			authenticator := sig.NewV4Authenticator(req)
			sigContext, err := authenticator.Parse()
			if err != nil {
				t.Fatalf("failed to parse request with STREAMING-UNSIGNED-PAYLOAD-TRAILER: %v", err)
			}

			// Verify the signature
			err = authenticator.Verify(&modelCred)
			
			// For invalid trailer cases, we expect an error
			if tc.expectedError {
				if err == nil {
					t.Errorf("expected an error for invalid trailer, but got none")
				} else if !errors.Is(err, sig.ErrUnsupportedChecksum) {
					t.Errorf("expected ErrUnsupportedChecksumAlgorithm error, got: %v", err)
				}
				return // Skip the rest of the test for invalid cases
			} else if err != nil {
				t.Errorf("failed to verify request with STREAMING-UNSIGNED-PAYLOAD-TRAILER: %v", err)
				return
			}

			// Check that it properly identified the auth type
			if sigContext.GetAccessKeyID() != testID {
				t.Errorf("expected access key ID to be %s, got %s", testID, sigContext.GetAccessKeyID())
			}

			// Verify that content length was properly set to decoded length
			if req.ContentLength != int64(decodedContentLength) {
				t.Errorf("expected content length to be %d, got %d", decodedContentLength, req.ContentLength)
			}

			// Read the body to verify the chunks are correctly parsed
			bodyData, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("failed to read parsed body: %v", err)
			}

			// Verify that the decoded body contains exactly what we expect
			if !bytes.Equal(bodyData, combinedData) {
				t.Errorf("body data doesn't match expected content\nExpected: %s\nGot: %s",
					truncateForDisplay(string(combinedData)), truncateForDisplay(string(bodyData)))
			}
		})
	}
}
