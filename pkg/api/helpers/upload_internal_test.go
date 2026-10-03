package helpers

import (
	"fmt"
	"testing"
	"time"
)

func TestPresignedURLExpired(t *testing.T) {
	const (
		s3URL  = "https://bucket.s3.amazonaws.com/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-Signature=sig"
		gcsURL = "https://storage.googleapis.com/bucket/key?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Date=%s&X-Goog-Expires=%d&X-Goog-Signature=sig"
	)
	const expiresSec = 900
	signedAt := time.Now().UTC().Format(signedDateFormat)
	staleSignedAt := time.Now().UTC().Add(-2 * expiresSec * time.Second).Format(signedDateFormat)

	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"s3_valid", fmt.Sprintf(s3URL, signedAt, expiresSec), false},
		{"s3_expired", fmt.Sprintf(s3URL, staleSignedAt, expiresSec), true},
		{"gcs_valid", fmt.Sprintf(gcsURL, signedAt, expiresSec), false},
		{"gcs_expired", fmt.Sprintf(gcsURL, staleSignedAt, expiresSec), true},
		// anything we cannot read an expiry out of is refreshed rather than risked
		{"no_signing_params", "https://storage.googleapis.com/bucket/key", true},
		{"bad_date", fmt.Sprintf(gcsURL, "not-a-date", expiresSec), true},
		{"invalid_url", "://not a url", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := presignedURLExpired(tt.url); got != tt.expected {
				t.Errorf("presignedURLExpired()=%v, expected %v", got, tt.expected)
			}
		})
	}
}
