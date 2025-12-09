package esti

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	// testS3Region is the AWS region used for S3 signature testing.
	// This must match the region used in testutil.SetupTestS3Client.
	testS3Region = "us-east-1"
)

// TestS3PresignedURLExpirationWithCustomTime tests presigned URL expiration validation
// using the v4.Signer.PresignHTTP method which allows controlling the signing time
func TestS3PresignedURLExpirationWithCustomTime(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	blockStoreType := viper.GetString(ViperBlockstoreType)
	if blockStoreType != "s3" {
		t.Skip("Skipping test - only S3 blockstore type is supported for presigned URL expiration validation")
	}

	// Upload a test file to use for presigned URL tests
	minioClient := newMinioClient(t, credentials.NewStaticV4)
	testContent := "test content for presigned URL expiration"
	testPath := "main/presign-expiry-test"

	_, err := minioClient.PutObject(ctx, repo, testPath, strings.NewReader(testContent), int64(len(testContent)), minio.PutObjectOptions{})
	require.NoError(t, err, "failed to upload test file")

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	s3Endpoint := viper.GetString("s3_endpoint")

	// Create v4 signer
	signer := v4.NewSigner()
	creds := aws.Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}

	// Helper function to create presigned URL with custom signing time
	createPresignedURL := func(signingTime time.Time, expiryDuration time.Duration) (string, error) {
		// Build the request URL properly using url.URL
		endpoint := s3Endpoint
		if !strings.HasPrefix(endpoint, "http") {
			if viper.GetBool("s3_endpoint_secure") {
				endpoint = "https://" + endpoint
			} else {
				endpoint = "http://" + endpoint
			}
		}

		baseURL, err := url.Parse(endpoint)
		if err != nil {
			return "", fmt.Errorf("failed to parse endpoint: %w", err)
		}

		// Construct the path: /bucket/key
		baseURL.Path = fmt.Sprintf("/%s/%s", repo, testPath)

		// Create HTTP request
		req, err := http.NewRequest("GET", baseURL.String(), nil)
		if err != nil {
			return "", err
		}

		// Add required query parameters for presigning
		q := req.URL.Query()
		q.Set("X-Amz-Expires", strconv.FormatInt(int64(expiryDuration.Seconds()), 10))
		req.URL.RawQuery = q.Encode()

		// Calculate payload hash (UNSIGNED-PAYLOAD for presigned URLs)
		payloadHash := "UNSIGNED-PAYLOAD"

		// Presign the request with custom signing time
		signedURI, _, err := signer.PresignHTTP(
			context.Background(),
			creds,
			req,
			payloadHash,
			"s3",
			testS3Region,
			signingTime,
			func(opts *v4.SignerOptions) {
				opts.DisableURIPathEscaping = true // S3 doesn't need additional escaping
			},
		)
		if err != nil {
			return "", err
		}

		return signedURI, nil
	}

	t.Run("valid_presigned_url", func(t *testing.T) {
		// Create a presigned URL signed now with 1 hour expiry
		presignedURL, err := createPresignedURL(time.Now(), time.Hour)
		require.NoError(t, err, "failed to create presigned URL")

		// The presigned URL should work
		resp, err := http.Get(presignedURL)
		require.NoError(t, err, "failed to GET presigned URL")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "expected successful response from valid presigned URL")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "failed to read response body")
		require.Equal(t, testContent, string(body), "content mismatch")
	})

	t.Run("expired_presigned_url", func(t *testing.T) {
		// Create a presigned URL signed 15 minutes ago with 10 minute expiry
		// This means it expired 5 minutes ago
		signTime := time.Now().Add(-15 * time.Minute)
		presignedURL, err := createPresignedURL(signTime, 10*time.Minute)
		require.NoError(t, err, "failed to create presigned URL")

		// The presigned URL should be expired
		resp, err := http.Get(presignedURL)
		require.NoError(t, err, "failed to GET presigned URL")
		defer resp.Body.Close()

		require.Equal(t, http.StatusForbidden, resp.StatusCode, "expected 403 Forbidden from expired presigned URL")
	})

	t.Run("presigned_url_within_clock_skew_tolerance", func(t *testing.T) {
		// Create a presigned URL signed 10 minutes in the future (within 15 minute tolerance)
		signTime := time.Now().Add(10 * time.Minute)
		presignedURL, err := createPresignedURL(signTime, time.Hour)
		require.NoError(t, err, "failed to create presigned URL")

		// The presigned URL should work despite being signed in the future
		resp, err := http.Get(presignedURL)
		require.NoError(t, err, "failed to GET presigned URL")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode,
			"expected successful response from URL signed within 15min clock skew tolerance")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "failed to read response body")
		require.Equal(t, testContent, string(body), "content mismatch")
	})

	t.Run("presigned_url_signed_too_far_in_future", func(t *testing.T) {
		// Create a presigned URL signed 20 minutes in the future (beyond 15 minute tolerance)
		signTime := time.Now().Add(20 * time.Minute)
		presignedURL, err := createPresignedURL(signTime, time.Hour)
		require.NoError(t, err, "failed to create presigned URL")

		// The presigned URL should be rejected
		resp, err := http.Get(presignedURL)
		require.NoError(t, err, "failed to GET presigned URL")
		defer resp.Body.Close()

		require.Equal(t, http.StatusForbidden, resp.StatusCode,
			"expected 403 Forbidden from URL signed more than 15min in the future")
	})

	t.Run("expiry_calculated_from_signed_time_not_current_time", func(t *testing.T) {
		// Test that expiry is calculated from signed time, not current time
		// This proves the expiration countdown starts when the URL is signed,
		// not when it's accessed.
		const (
			timeInPast     = 2 * time.Second // How long ago the URL was "signed"
			expiryDuration = 4 * time.Second // Total expiry time from signing
			sleepDuration  = 3 * time.Second // How long to wait before second request
		)
		// Expected behavior:
		// - First request:  signed 2s ago + 4s expiry = 2s remaining → should work
		// - After 3s sleep: signed 5s ago + 4s expiry = expired 1s ago → should fail

		signTime := time.Now().Add(-timeInPast)
		presignedURL, err := createPresignedURL(signTime, expiryDuration)
		require.NoError(t, err, "failed to create presigned URL")

		// First request: URL should still be valid (2 seconds remaining)
		resp, err := http.Get(presignedURL)
		require.NoError(t, err, "failed to GET presigned URL")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode,
			"expected successful response - URL should not be expired yet")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "failed to read response body")
		require.Equal(t, testContent, string(body), "content mismatch")

		// Wait for the URL to expire
		time.Sleep(sleepDuration)

		// Second request: URL should now be expired (1 second past expiration)
		resp2, err := http.Get(presignedURL)
		require.NoError(t, err, "failed to GET presigned URL")
		defer resp2.Body.Close()

		require.Equal(t, http.StatusForbidden, resp2.StatusCode,
			"expected 403 Forbidden - URL should be expired after waiting")
	})
}
