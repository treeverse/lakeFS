package authentication

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	AWSAuthVersion      = "2011-06-15"
	AWSAuthMethod       = http.MethodPost
	AWSAuthAction       = "GetCallerIdentity"
	AWSAuthAlgorithm    = "AWS4-HMAC-SHA256"
	StsGlobalEndpoint   = "sts.amazonaws.com"
	AWSAuthActionKey    = "Action"
	AWSAuthVersionKey   = "Version"
	AWSAuthAlgorithmKey = "X-Amz-Algorithm"
	//nolint:gosec
	AWSAuthCredentialKey = "X-Amz-Credential"
	AWSAuthDateKey       = "X-Amz-Date"
	AWSAuthExpiresKey    = "X-Amz-Expires"
	//nolint:gosec
	AWSAuthSecurityTokenKey  = "X-Amz-Security-Token"
	AWSAuthSignedHeadersKey  = "X-Amz-SignedHeaders"
	AWSAuthSignatureKey      = "X-Amz-Signature"
	AWSDatetimeFormat        = "20060102T150405Z"
	AWSCredentialTimeFormat  = "20060102"
	AWSDefaultSTSLoginExpire = 15 * time.Minute
)

var ErrAWSCredentialsExpired = errors.New("AWS credentials expired")
var ErrRetrievingToken = errors.New("failed to retrieve token")

type LoginResponse struct {
	Token string
}

type IDPProvider interface {
	Login() (LoginResponse, error)
}

type AWSProvider struct {
	params         AWSIAMParams
	serverEndpoint string
	client         *http.Client
}

type IdentityTokenInfo struct {
	Method             string   `json:"method"`
	Host               string   `json:"host"`
	Region             string   `json:"region"`
	Action             string   `json:"action"`
	Date               string   `json:"date"`
	ExpirationDuration string   `json:"expiration_duration"`
	AccessKeyID        string   `json:"access_key_id"`
	Signature          string   `json:"signature"`
	SignedHeaders      []string `json:"signed_headers"`
	Version            string   `json:"version"`
	Algorithm          string   `json:"algorithm"`
	SecurityToken      string   `json:"security_token"`
}

type AWSIAMParams struct {
	ProviderType        string
	TokenRequestHeaders map[string]string
	URLPresignTTL       time.Duration
	TokenTTL            time.Duration
}

func NewAWSProvider(params AWSIAMParams, serverEndpoint string, httpClient *http.Client) *AWSProvider {
	return &AWSProvider{
		params:         params,
		serverEndpoint: serverEndpoint,
		client:         httpClient,
	}
}

func (p *AWSProvider) Login() (LoginResponse, error) {
	jwt, err := getJWT(&p.params, p.serverEndpoint, p.client)
	resp := LoginResponse{Token: jwt}
	return resp, err
}

func getJWT(params *AWSIAMParams, serverEndpoint string, httpClient *http.Client) (string, error) {
	ctx := context.TODO()
	identityToken, err := getIdentityToken(ctx, params)
	if err != nil {
		return "", err
	}

	client, err := apigen.NewClientWithResponses(
		serverEndpoint,
		apigen.WithHTTPClient(httpClient),
	)
	if err != nil {
		return "", err
	}

	tokenTTL := int(params.TokenTTL.Seconds())
	externalLoginInfo := apigen.ExternalLoginInformation{
		IdentityRequest: map[string]interface{}{
			"identity_token": identityToken,
		},
		TokenExpirationDuration: &tokenTTL,
	}
	externalPrincipalLoginResp, err := client.ExternalPrincipalLoginWithResponse(ctx, apigen.ExternalPrincipalLoginJSONRequestBody(externalLoginInfo))
	if err != nil {
		return "", err
	}
	if externalPrincipalLoginResp == nil || externalPrincipalLoginResp.JSON200 == nil {
		return "", ErrRetrievingToken
	}
	return externalPrincipalLoginResp.JSON200.Token, nil
}

func getIdentityToken(ctx context.Context, params *AWSIAMParams) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return "", err
	}
	if creds.Expired() {
		return "", ErrAWSCredentialsExpired
	}
	stsClient := sts.NewFromConfig(cfg)
	stsPresignClient := sts.NewPresignClient(stsClient, func(o *sts.PresignOptions) {
		o.ClientOptions = append(o.ClientOptions, func(opts *sts.Options) {
			opts.ClientLogMode = aws.LogSigning
		})
	})

	presignGetCallerIdentityResp, err := stsPresignClient.PresignGetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{},
		sts.WithPresignClientFromClientOptions(sts.WithAPIOptions(setHTTPHeaders(params.TokenRequestHeaders, params.URLPresignTTL))),
	)
	if err != nil {
		return "", err
	}

	parsedURL, err := url.Parse(presignGetCallerIdentityResp.URL)
	if err != nil {
		return "", err
	}

	queryParams := parsedURL.Query()
	credentials := queryParams.Get(AWSAuthCredentialKey)
	splitedCreds := strings.Split(credentials, "/")
	calculatedRegion := splitedCreds[2]
	identityTokenInfo := IdentityTokenInfo{
		Method:             "POST",
		Host:               parsedURL.Host,
		Region:             calculatedRegion,
		Action:             AWSAuthAction,
		Date:               queryParams.Get(AWSAuthDateKey),
		ExpirationDuration: queryParams.Get(AWSAuthExpiresKey),
		AccessKeyID:        creds.AccessKeyID,
		Signature:          queryParams.Get(AWSAuthSignatureKey),
		SignedHeaders:      strings.Split(queryParams.Get(AWSAuthSignedHeadersKey), ";"),
		Version:            queryParams.Get(AWSAuthVersionKey),
		Algorithm:          queryParams.Get(AWSAuthAlgorithmKey),
		SecurityToken:      queryParams.Get(AWSAuthSecurityTokenKey),
	}

	marshaledIdentityTokenInfo, _ := json.Marshal(identityTokenInfo)
	encodedIdentityTokenInfo := base64.StdEncoding.EncodeToString(marshaledIdentityTokenInfo)
	return encodedIdentityTokenInfo, nil
}

func setHTTPHeaders(requestHeaders map[string]string, ttl time.Duration) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("AddHeaders", func(
			ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
		) (
			middleware.BuildOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.Method = "POST"
				for header, value := range requestHeaders {
					req.Header.Add(header, value)
				}
				queryParams := req.Request.URL.Query()
				queryParams.Set(AWSAuthExpiresKey, fmt.Sprintf("%d", int(ttl.Seconds())))
				req.Request.URL.RawQuery = queryParams.Encode()
			}
			return next.HandleBuild(ctx, in)
		}), middleware.Before)
	}
}
