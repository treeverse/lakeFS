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
	fmt.Println("\n\n", url)
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

func TestNewIAMAuthParams(t *testing.T) {
	params := NewIAMAuthParams("")
	require.Equal(t, params.TokenTTL, 3600*time.Minute)
	require.Equal(t, params.RefreshInterval, 5*time.Minute)
	require.Equal(t, params.TokenRequestHeaders["X-LakeFS-Server-ID"], "")

	newheaders := map[string]string{"header": "hallo"}
	newparams := NewIAMAuthParams("host", WithRefreshInterval(13*time.Minute), WithTokenTTL(9*time.Minute), WithProviderType("aws_iam"), WithTokenRequestHeaders(newheaders))
	require.Equal(t, newparams.TokenTTL, 9*time.Minute)
	require.Equal(t, newparams.RefreshInterval, 13*time.Minute)
	require.Equal(t, newparams.ProviderType, "aws_iam")
	require.NotContains(t, newparams.TokenRequestHeaders["X-LakeFS-Server-ID"], "host")
	require.Equal(t, newparams.TokenRequestHeaders["header"], "hallo")
}
