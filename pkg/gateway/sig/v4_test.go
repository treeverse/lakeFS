package sig_test

import (
	"bytes"
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
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth/model"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
)

var mockCreds = &model.Credential{
	BaseCredential: model.BaseCredential{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	},
}

func TestV4AuthenticationFailures(t *testing.T) {
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

func TestV4SignedPayloadVerification(t *testing.T) {
	// This test verifies successful V4 signature verification with a signed payload
	// Based on the Amazon single chunk example from AWS documentation
	// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

	const (
		host    = "examplebucket.s3.amazonaws.com"
		path    = "test$file.text"
		region  = "us-east-1"
		service = "s3"
	)

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/%s", host, path), nil)
	if err != nil {
		t.Fatalf("failed to create http.Request, got %v", err)
	}

	payload := []byte("Welcome to Amazon S3.")
	h := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(h[:])

	// Use current time for signing
	sigTime := time.Now().UTC()

	req.Header.Set("Host", host)
	req.Header.Set("X-Amz-Date", sigTime.Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	req.Header.Set("Date", sigTime.Format(time.RFC1123))

	creds := aws.Credentials{
		AccessKeyID:     mockCreds.AccessKeyID,
		SecretAccessKey: mockCreds.SecretAccessKey,
	}
	signer := v4.NewSigner()
	err = signer.SignHTTP(t.Context(), creds, req, payloadHash, service, region, sigTime)
	if err != nil {
		t.Fatalf("Failed to sign request: %v", err)
	}

	req.Body = io.NopCloser(bytes.NewReader(payload))

	authenticator := sig.NewV4Authenticator(req)
	_, err = authenticator.Parse()
	require.NoError(t, err, "failed to parse auth header")

	err = authenticator.Verify(&model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:     mockCreds.AccessKeyID,
			SecretAccessKey: mockCreds.SecretAccessKey,
			IssuedDate:      sigTime,
		},
	})
	require.NoError(t, err, "failed to verify signature")

	// Read and verify the body
	bodyData, err := io.ReadAll(req.Body)
	require.NoError(t, err, "failed to read body")
	require.Equal(t, payload, bodyData, "body mismatch")
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
			ExpectedReadError: gatewayerrors.ErrSignatureDoesNotMatch,
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
	ctx := t.Context()
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

// setupAndSignStreamingRequest creates and signs a streaming request, returning the request,
// seed signature, and credential for further chunk signature calculations.
func setupAndSignStreamingRequest(t *testing.T, accessKey, secretKey, host, path, region, service string, sigTime time.Time, decodedContentLength int) (*http.Request, string, *model.Credential) {
	t.Helper()

	// Create request
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/%s", host, path), nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Host", host)
	req.Header.Set("X-Amz-Date", sigTime.Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprintf("%d", decodedContentLength))

	creds := aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	signer := v4.NewSigner()
	err = signer.SignHTTP(t.Context(), creds, req, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", service, region, sigTime)
	require.NoError(t, err, "failed to sign request")

	// Get the seed signature from the Authorization header
	authHeader := req.Header.Get("Authorization")
	_, seedSig, found := strings.Cut(authHeader, "Signature=")
	require.True(t, found, "Authorization header missing Signature field")

	// Create credential for chunk signature calculation
	modelCred := &model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			IssuedDate:      sigTime,
		},
	}

	return req, seedSig, modelCred
}

func TestStreaming(t *testing.T) {
	const (
		ID      = "AKIAIOSFODNN7EXAMPLE"
		SECRET  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		host    = "s3.amazonaws.com"
		path    = "examplebucket/chunkObject.txt"
		region  = "us-east-1"
		service = "s3"
	)

	testCases := []struct {
		name              string
		corruptLastByte   bool
		expectedReadError error
	}{
		{
			name:              "successful signature verification with valid chunks",
			corruptLastByte:   false,
			expectedReadError: nil,
		},
		{
			name:              "signature mismatch on corrupted chunk",
			corruptLastByte:   true,
			expectedReadError: gatewayerrors.ErrSignatureDoesNotMatch,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Define chunk data
			const chunk1Size = 65536 // 64KB
			const chunk2Size = 1024  // 1KB
			decodedContentLength := chunk1Size + chunk2Size
			chunk1Data := bytes.Repeat([]byte("a"), chunk1Size)
			chunk2Data := bytes.Repeat([]byte("a"), chunk2Size)

			// Use current time for signing
			sigTime := time.Now().UTC()

			// Setup and sign the streaming request
			req, seedSig, modelCred := setupAndSignStreamingRequest(t, ID, SECRET, host, path, region, service, sigTime, decodedContentLength)

			// Calculate chunk signatures with the correct data
			chunk1Hash := sha256.Sum256(chunk1Data)
			chunk1Sig := sig.GetChunkSignature(modelCred, seedSig, region, service, sigTime, hex.EncodeToString(chunk1Hash[:]))

			chunk2Hash := sha256.Sum256(chunk2Data)
			chunk2Sig := sig.GetChunkSignature(modelCred, chunk1Sig, region, service, sigTime, hex.EncodeToString(chunk2Hash[:]))

			emptyHash := sha256.Sum256([]byte{})
			finalSig := sig.GetChunkSignature(modelCred, chunk2Sig, region, service, sigTime, hex.EncodeToString(emptyHash[:]))

			// Optionally corrupt the last byte of chunk2 after signing
			if tc.corruptLastByte {
				chunk2Data[len(chunk2Data)-1] = 'b'
			}

			// Build the chunked body
			var body bytes.Buffer
			body.WriteString(fmt.Sprintf("%x;chunk-signature=%s\r\n", chunk1Size, chunk1Sig))
			body.Write(chunk1Data)
			body.WriteString("\r\n")

			body.WriteString(fmt.Sprintf("%x;chunk-signature=%s\r\n", chunk2Size, chunk2Sig))
			body.Write(chunk2Data)
			body.WriteString("\r\n")

			body.WriteString(fmt.Sprintf("0;chunk-signature=%s\r\n\r\n", finalSig))

			// Update Content-Length with actual body size
			req.Header.Set("Content-Length", fmt.Sprintf("%d", body.Len()))
			req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

			authenticator := sig.NewV4Authenticator(req)
			_, err := authenticator.Parse()
			require.NoError(t, err, "failed to parse auth header")

			err = authenticator.Verify(modelCred)
			require.NoError(t, err, "failed to verify signature")

			require.Equal(t, int64(decodedContentLength), req.ContentLength, "content length mismatch")

			bodyContent, err := io.ReadAll(req.Body)
			require.ErrorIs(t, err, tc.expectedReadError, "unexpected read error: %v", err)
			if tc.expectedReadError != nil {
				return
			}

			expectedContent := append(chunk1Data, chunk2Data...)
			require.Equal(t, expectedContent, bodyContent, "body content mismatch")
		})
	}
}

func TestUnsignedPayload(t *testing.T) {
	const (
		testID     = "AKIAIOSFODNN7EXAMPLE"
		testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region     = "us-east-1"
		service    = "s3"
	)

	req, err := http.NewRequest(http.MethodHead, "https://s3.amazonaws.com/examplebucket/test.txt", nil)
	require.NoError(t, err, "failed to create request")

	req.Header.Set("Host", "s3.amazonaws.com")
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	creds := aws.Credentials{
		AccessKeyID:     testID,
		SecretAccessKey: testSecret,
	}

	// Use the current time for signing
	sigTime := time.Now().UTC()
	signer := v4.NewSigner()
	err = signer.SignHTTP(t.Context(), creds, req, "UNSIGNED-PAYLOAD", service, region, sigTime)
	require.NoError(t, err, "failed to sign request")

	authenticator := sig.NewV4Authenticator(req)
	_, err = authenticator.Parse()
	require.NoError(t, err, "failed to parse auth header")

	err = authenticator.Verify(&model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:     testID,
			SecretAccessKey: testSecret,
			IssuedDate:      sigTime,
		},
	})
	require.NoError(t, err, "failed to verify signature")
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
		name                string
		method              string
		host                string
		path                string
		trailerHeader       sig.ChecksumAlgorithm
		trailerContent      string
		chunkSize           int
		totalChunks         int
		chunkData           [][]byte
		expectedVerifyError error
		expectedReadError   error
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
		},
		{
			name:           "Invalid TrailerHeader",
			method:         http.MethodPut,
			host:           "s3.amazonaws.com",
			path:           "examplebucket/trailertest.txt",
			trailerHeader:  sig.ChecksumAlgorithmInvalid,
			trailerContent: "x-amz-checksum-crc32:%s\r\n\r\n",
			chunkSize:      64,
			totalChunks:    1,
			chunkData: [][]byte{
				bytes.Repeat([]byte("a"), 64),
			},
			expectedVerifyError: sig.ErrUnsupportedChecksum,
		},
		{
			name:           "Invalid TrailerContent",
			method:         http.MethodPut,
			host:           "s3.amazonaws.com",
			path:           "examplebucket/trailertest.txt",
			trailerHeader:  sig.ChecksumAlgorithmCRC32,
			trailerContent: "x-amz-checksum-sha1:%s\r\n\r\n",
			chunkSize:      64,
			totalChunks:    1,
			chunkData: [][]byte{
				bytes.Repeat([]byte("a"), 64),
			},
			expectedReadError: sig.ErrChecksumTypeMismatch,
		},
	}

	const (
		testID     = "AKIAIOSFODNN7EXAMPLE"
		testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region     = "us-east-1"
		service    = "s3"
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use the current time for signing to pass timestamp validation
			sigTime := time.Now().UTC()

			// Create a request
			req, err := http.NewRequest(tc.method, fmt.Sprintf("https://%s/%s", tc.host, tc.path), nil)
			if err != nil {
				t.Fatal(err)
			}

			// Set required headers for unsigned trailers
			req.Header.Set("X-Amz-Date", sigTime.Format("20060102T150405Z"))
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
			err = signer.SignHTTP(t.Context(), creds, req, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", service, region, sigTime)
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
			if tc.expectedVerifyError == nil && tc.expectedReadError == nil {
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
					IssuedDate:      sigTime,
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
			if !errors.Is(err, tc.expectedVerifyError) {
				t.Fatalf("unexpected verify error: expected %v but got %v", tc.expectedVerifyError, err)
			} else if tc.expectedVerifyError != nil {
				t.Logf("got expected error: %v", tc.expectedVerifyError)
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
			if !errors.Is(err, tc.expectedReadError) {
				t.Fatalf("unexpected read error: expected %v but got %v", tc.expectedReadError, err)
			} else if tc.expectedReadError != nil {
				t.Logf("got expected error: %v", tc.expectedReadError)
				return
			}

			// Verify that the decoded body contains exactly what we expect
			if !bytes.Equal(bodyData, combinedData) {
				t.Errorf("body data doesn't match expected content\nExpected: %s\nGot: %s",
					truncateForDisplay(string(combinedData)), truncateForDisplay(string(bodyData)))
			}
		})
	}
}
