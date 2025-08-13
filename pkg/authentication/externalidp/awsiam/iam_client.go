package awsiam

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	IdentityTokenKey = "identity_token"
)

type ExternalPrincipalLoginClient struct {
	Client *apigen.ClientWithResponses
}

type ExternalPrincipalLoginCaller interface {
	ExternalPrincipalLogin(ctx context.Context, loginInfo apigen.ExternalLoginInformation) (*apigen.AuthenticationToken, error)
}

// WithAWSIAMRoleAuthProviderOption will add authentication provider into the GetClient request, which will return a client authenticated with IAM.
func WithAWSIAMRoleAuthProviderOption(params *IAMAuthParams, logger logging.Logger, client ExternalPrincipalLoginCaller, initialToken *apigen.AuthenticationToken, presignClientOpts ...func(*sts.PresignOptions)) apigen.ClientOption {
	awsProvider := NewSecurityProviderAWSIAMRole(logger, params, client, initialToken, presignClientOpts...)
	return apigen.WithRequestEditorFn(awsProvider.Intercept)
}

func (c *ExternalPrincipalLoginClient) ExternalPrincipalLogin(ctx context.Context, loginInfo apigen.ExternalLoginInformation) (*apigen.AuthenticationToken, error) {
	requestBody := apigen.ExternalPrincipalLoginJSONRequestBody(loginInfo)
	resp, err := c.Client.ExternalPrincipalLoginWithResponse(ctx, requestBody)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 != nil {
		return resp.JSON200, nil
	}
	return nil, fmt.Errorf("%w (status: %d)", authentication.ErrExternalLoginFailed, resp.StatusCode())
}

type SecurityProviderAWSIAMRole struct {
	Logger              logging.Logger
	AuthenticationToken *apigen.AuthenticationToken
	Params              *IAMAuthParams
	// lakeFS unauthenticated client to perform login request. Because of legacy we
	// have in the generated client code it must contain authrozation header (e.g empty access/secret key id)
	Client            ExternalPrincipalLoginCaller
	PresignClientOpts []func(*sts.PresignOptions)
}

func NewSecurityProviderAWSIAMRole(logger logging.Logger, params *IAMAuthParams, client ExternalPrincipalLoginCaller, optionalInitialToken *apigen.AuthenticationToken, presignClientOpts ...func(*sts.PresignOptions)) *SecurityProviderAWSIAMRole {
	return &SecurityProviderAWSIAMRole{
		Logger:              logger,
		AuthenticationToken: optionalInitialToken,
		Params:              params,
		Client:              client,
		PresignClientOpts:   presignClientOpts,
	}
}
func (s *SecurityProviderAWSIAMRole) Intercept(ctx context.Context, req *http.Request) error {
	if s.AuthenticationToken == nil || s.AuthenticationToken.Token == "" ||
		(s.AuthenticationToken.TokenExpiration != nil &&
			time.Now().Add(s.Params.RefreshInterval).After(time.Unix(*s.AuthenticationToken.TokenExpiration, 0))) {
		s.Logger.Debug("authentication token is missing or expired, fetching a new one")
		token, err := s.GetLakeFSTokenFromAWS(ctx)
		if err != nil {
			s.Logger.WithError(err).Error("failed generating a new session token from aws")
			return err
		}
		s.AuthenticationToken = token
		s.Logger.WithField("expiry", time.Unix(*s.AuthenticationToken.TokenExpiration, 0)).Debug("success renewing session token")
	}
	req.Header.Set("Authorization", "Bearer "+s.AuthenticationToken.Token)
	return nil
}

func (s *SecurityProviderAWSIAMRole) GetLakeFSTokenFromAWS(ctx context.Context) (*apigen.AuthenticationToken, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	stsClient := sts.NewFromConfig(cfg)
	tokenInfo, err := GenerateIdentityTokenInfo(ctx, s.Params, stsClient, s.PresignClientOpts...)
	if err != nil {
		return nil, fmt.Errorf("generating identity token: %w", err)
	}
	externalLoginInfo, err := NewExternalLoginInformationFromAWS(tokenInfo, s.Params)
	if err != nil {
		return nil, fmt.Errorf("marshaling login info: %w", err)
	}
	res, err := s.Client.ExternalPrincipalLogin(ctx, *externalLoginInfo)
	if err != nil {
		return nil, fmt.Errorf("calling external principal login: %w", err)
	}
	return res, nil
}

func NewExternalLoginInformationFromAWS(tokenInfo *AWSIdentityTokenInfo, params *IAMAuthParams) (*apigen.ExternalLoginInformation, error) {
	marshaledIdentityTokenInfo, err := json.Marshal(tokenInfo)
	if err != nil {
		return nil, err
	}
	encodedIdentityTokenInfo := base64.StdEncoding.EncodeToString(marshaledIdentityTokenInfo)

	tokenTTL := int(params.TokenTTL.Seconds())
	externalLoginInfo := &apigen.ExternalLoginInformation{
		IdentityRequest: map[string]any{
			IdentityTokenKey: encodedIdentityTokenInfo,
		},
		TokenExpirationDuration: &tokenTTL,
	}
	return externalLoginInfo, nil
}
