package awsiam

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/require"
)

// A custom credentials provider for mocking
type mockCredentialsProvider struct {
	creds aws.Credentials
	err   error
}

func (m mockCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return m.creds, m.err
}

func TestGetCreds_Success(t *testing.T) {
	expectedCreds := aws.Credentials{
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "SECRET",
		SessionToken:    "SESSION",
		Source:          "Mock",
	}

	cfg := aws.Config{
		Credentials: mockCredentialsProvider{
			creds: expectedCreds,
			err:   nil,
		},
	}

	creds, err := GetCredsIfValid(context.Background(), &cfg)
	require.NoError(t, err)
	require.Equal(t, expectedCreds.AccessKeyID, creds.AccessKeyID)
	require.Equal(t, expectedCreds.SecretAccessKey, creds.SecretAccessKey)
}

func TestGetCreds_Expired(t *testing.T) {
	expiredCreds := aws.Credentials{
		AccessKeyID: "EXPIRED",
		CanExpire:   true,
		Expires:     time.Now().Add(-1 * time.Minute),
	}

	cfg := aws.Config{
		Credentials: mockCredentialsProvider{
			creds: expiredCreds,
			err:   nil,
		},
	}

	creds, err := GetCredsIfValid(context.Background(), &cfg)
	require.Error(t, err)
	require.Equal(t, ErrAWSCredentialsExpired, err)
	require.Nil(t, creds)
}

func TestGetCreds_Failure(t *testing.T) {
	cfg := aws.Config{
		Credentials: mockCredentialsProvider{
			err: errors.New("failed to load creds"),
		},
	}

	creds, err := GetCredsIfValid(context.Background(), &cfg)
	require.Error(t, err)
	require.EqualError(t, err, "failed to load creds")
	require.Nil(t, creds)
}

func TestGetPresignedURL_Integration(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	require.NoError(t, err)

	creds, err := GetCredsIfValid(context.TODO(), &cfg)
	require.NoError(t, err)

	params := &IAMAuthParams{
		TokenRequestHeaders: map[string]string{
			"X-Custom-Header": "test",
		},
		URLPresignTTL: 10 * time.Minute,
	}

	url, err := GeneratePresignedURL(context.TODO(), params, &cfg, creds)
	fmt.Println("url!!", url)
	require.NoError(t, err)
	require.Contains(t, url, "sts.")            // loosely validates STS domain
	require.Contains(t, url, "amazonaws.com")   // ensures it's AWS
	require.Contains(t, url, "X-Amz-Signature") // ensures it's signed
}
func TestGeneratePresignedURL_Integration(t *testing.T) {
	expiredCreds := aws.Credentials{
		AccessKeyID: "valid",
		CanExpire:   true,
		Expires:     time.Now().Add(+1 * time.Minute),
	}

	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: mockCredentialsProvider{
			creds: expiredCreds,
			err:   nil,
		},
	}

	creds, err := GetCredsIfValid(context.Background(), &cfg)
	require.NoError(t, err)
	require.False(t, creds.Expired())

	params := &IAMAuthParams{
		TokenRequestHeaders: map[string]string{
			"X-Custom-Test": "true",
			"a-nice-header": "yes-please",
		},
		URLPresignTTL: 6 * time.Minute,
	}

	url, err := GeneratePresignedURL(context.Background(), params, &cfg, creds)
	require.NoError(t, err)
	require.NotEmpty(t, url)

	fmt.Println("url!!", url)

	// Basic validations
	require.Contains(t, url, "sts.")
	require.Contains(t, url, "us-east-1")
	require.Contains(t, url, "X-Amz-Signature")
	require.Contains(t, url, "X-Amz-Credential")
	require.Contains(t, url, "X-Amz-Algorithm")
	require.Contains(t, url, "X-Amz-Date")
	require.Contains(t, url, "X-Amz-Expires=360")
	require.Contains(t, url, "a-nice-header")
	require.Contains(t, url, "x-custom-test")
}
