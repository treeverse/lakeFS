package awsiam

import (
	"context"
	"fmt"
	"net/url"
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

func TestPresignGetCallerIdentityFromAuthParams(t *testing.T) {
	numSeconds := 360
	validCreds := aws.Credentials{
		AccessKeyID:     "accesKey",
		SecretAccessKey: "secretKey",
		SessionToken:    "securityToken",
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

	presignedURL, err := PresignGetCallerIdentityFromAuthParams(context.TODO(), params, stsClient)
	require.NoError(t, err)
	u, err := url.Parse(presignedURL)
	require.NoError(t, err)

	q := u.Query()

	require.Equal(t, fmt.Sprintf("%d", numSeconds), q.Get("X-Amz-Expires"))
	require.Equal(t, "AWS4-HMAC-SHA256", q.Get("X-Amz-Algorithm"))
	require.NotEmpty(t, q.Get("X-Amz-Signature"))
	require.Equal(t, "accesKey/20250514/us-east-1/sts/aws4_request", q.Get("X-Amz-Credential"))
	require.NotEmpty(t, q.Get("X-Amz-Date"))
	require.Equal(t, "a-nice-header;host;x-custom-test", q.Get("X-Amz-SignedHeaders"))
	require.Contains(t, q.Get("X-Amz-Security-Token"), "securityToken")
	require.Equal(t, "sts.us-east-1.amazonaws.com", u.Host)

}

func TestNewIAMAuthParams(t *testing.T) {
	thirteenM := 13 * time.Minute
	nineM := 9 * time.Minute
	params := NewIAMAuthParams("")
	require.Equal(t, params.TokenTTL, DefaultTokenTTL)
	require.Equal(t, params.RefreshInterval, DefaultRefreshInterval)
	require.Equal(t, params.TokenRequestHeaders[HostServerIDHeader], "")

	newheaders := map[string]string{"header": "hallo"}
	newparams := NewIAMAuthParams("host", WithRefreshInterval(thirteenM), WithTokenTTL(nineM), WithTokenRequestHeaders(newheaders))
	require.Equal(t, newparams.TokenTTL, nineM)
	require.Equal(t, newparams.RefreshInterval, thirteenM)
	require.NotContains(t, newparams.TokenRequestHeaders[HostServerIDHeader], "host")
	require.Equal(t, newparams.TokenRequestHeaders["header"], "hallo")
}
