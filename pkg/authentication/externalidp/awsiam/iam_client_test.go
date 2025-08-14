package awsiam_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/authentication/externalidp/awsiam"
	"github.com/treeverse/lakefs/pkg/logging"
)

var errTokenGenerationFailed = fmt.Errorf("token generation failed")

func makeAuthToken(token string, expiresInSeconds int64) *apigen.AuthenticationToken {
	var expiry *int64
	e := time.Now().Add(time.Duration(expiresInSeconds) * time.Second).Unix()
	expiry = &e

	return &apigen.AuthenticationToken{
		Token:           token,
		TokenExpiration: expiry,
	}
}

type testExternalLoginClient struct {
	shouldFail bool
	token      string
}

func (t *testExternalLoginClient) ExternalPrincipalLogin(ctx context.Context, loginInfo apigen.ExternalLoginInformation) (*apigen.AuthenticationToken, error) {
	if t.shouldFail {
		return nil, errTokenGenerationFailed
	}
	return makeAuthToken(t.token, 3600), nil
}

func TestIntercept(t *testing.T) {
	logger := logging.Dummy()

	tests := []struct {
		name          string
		initialToken  *apigen.AuthenticationToken
		expectedError error
		expectBearer  string
	}{
		{
			name:          "valid token",
			initialToken:  makeAuthToken("valid-token", 3600),
			expectedError: nil,
			expectBearer:  "valid-token",
		},
		{
			name:          "nil token",
			initialToken:  nil,
			expectedError: nil,
			expectBearer:  "new-token",
		},
		{
			name:          "empty token string",
			initialToken:  makeAuthToken("", 3600),
			expectedError: nil,
			expectBearer:  "new-token",
		},
		{
			name:          "expired token",
			initialToken:  makeAuthToken("expired-token", -3600),
			expectedError: nil,
			expectBearer:  "new-token",
		},
		{
			name:          "after refresh interval",
			initialToken:  makeAuthToken("soon-expired-token", 60*5),
			expectedError: nil,
			expectBearer:  "new-token",
		},
		{
			name:          "renewal fails",
			initialToken:  nil,
			expectedError: errTokenGenerationFailed,
			expectBearer:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iamAuthParams := &awsiam.IAMAuthParams{
				RefreshInterval: 5 * time.Minute,
			}
			var testClient *testExternalLoginClient
			if tt.expectedError != nil {
				testClient = &testExternalLoginClient{
					shouldFail: true,
					token:      tt.expectBearer,
				}
			} else {
				testClient = &testExternalLoginClient{
					shouldFail: false,
					token:      tt.expectBearer,
				}
			}

			presignOpts := func(opts *sts.PresignOptions) {
				opts.ClientOptions = []func(*sts.Options){
					func(o *sts.Options) {
						o.Credentials = credentials.NewStaticCredentialsProvider("dummy-access", "dummy-secret", "")
						o.Region = "us-east-1"
					},
				}
			}
			provider := awsiam.NewSecurityProviderAWSIAMRole(logger, iamAuthParams, testClient, tt.initialToken, presignOpts)

			req := httptest.NewRequest("GET", "http://example.com", nil)
			err := provider.Intercept(context.Background(), req)

			if tt.expectedError != nil {
				require.ErrorIs(t, err, errTokenGenerationFailed)
				require.Empty(t, req.Header.Get("Authorization"))
				return
			}

			require.ErrorIs(t, err, nil)
			require.Equal(t, "Bearer "+tt.expectBearer, req.Header.Get("Authorization"))
		})
	}
}
