package awsiam

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
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

func TestGeneratePresignedURL_Integration(t *testing.T) {
	numSeconds := 360
	validCreds := aws.Credentials{
		AccessKeyID: "accesKey",
	}
	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: mockCredentialsProvider{
			creds: validCreds,
			err:   nil,
		},
	}
	stsClient := sts.NewFromConfig(cfg)

	params := &IAMAuthParams{
		TokenRequestHeaders: map[string]string{
			"X-Custom-Test": "true",
			"a-nice-header": "yes-please",
		},
		URLPresignTTL: time.Duration(numSeconds) * time.Second,
	}

	url, err := PresignGetCallerIdentityFromAuthParams(context.TODO(), params, stsClient)
	require.NoError(t, err)
	require.NotEmpty(t, url)
	// Basic validations
	require.Contains(t, url, "sts.")
	require.Contains(t, url, "us-east-1")
	require.Contains(t, url, "X-Amz-Signature")
	require.Contains(t, url, "X-Amz-Credential")
	require.Contains(t, url, "X-Amz-Algorithm")
	require.Contains(t, url, "X-Amz-Date")
	require.Contains(t, url, fmt.Sprintf("X-Amz-Expires=%d", numSeconds))
	require.Contains(t, url, "a-nice-header")
	require.Contains(t, url, "x-custom-test")
}
