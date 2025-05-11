package awsiam

import (
	"context"
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
)

const (
	authVersion       = "2011-06-15"
	authMethod        = http.MethodPost
	authAction        = "GetCallerIdentity"
	authAlgorithm     = "AWS4-HMAC-SHA256"
	stsGlobalEndpoint = "sts.amazonaws.com"
	authActionKey     = "Action"
	authVersionKey    = "Version"
	authAlgorithmKey  = "X-Amz-Algorithm"
	//nolint:gosec
	authCredentialKey = "X-Amz-Credential"
	authDateKey       = "X-Amz-Date"
	authExpiresKey    = "X-Amz-Expires"
	//nolint:gosec
	authSecurityTokenKey  = "X-Amz-Security-Token"
	authSignedHeadersKey  = "X-Amz-SignedHeaders"
	authSignatureKey      = "X-Amz-Signature"
	datetimeFormat        = "20060102T150405Z"
	credentialTimeFormat  = "20060102"
	defaultSTSLoginExpire = 15 * time.Minute
)

var ErrAWSCredentialsExpired = errors.New("AWS credentials expired")
var ErrRetrievingToken = errors.New("failed to retrieve token")

type AWSIdentityTokenInfo struct {
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
type AWSProvider struct {
	Params IAMAuthParams
}
type IAMAuthParams struct {
	ProviderType        string
	TokenRequestHeaders map[string]string
	URLPresignTTL       time.Duration
	TokenTTL            time.Duration
	RefreshInterval     time.Duration
}

func NewAWSProvider(params IAMAuthParams) *AWSProvider {
	return &AWSProvider{
		Params: params,
	}
}

func (p *AWSProvider) NewRequest() (*AWSIdentityTokenInfo, error) {
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return &AWSIdentityTokenInfo{}, err
	}
	creds, err := GetCreds(ctx, &cfg)
	if err != nil {
		return &AWSIdentityTokenInfo{}, err
	}
	url, err := GetPresignedURL(ctx, &p.Params, &cfg, creds)
	if err != nil {
		return &AWSIdentityTokenInfo{}, err
	}
	tokenInfo, err := NewIdentityTokenInfo(creds, url)
	if err != nil {
		return &AWSIdentityTokenInfo{}, err
	}
	return tokenInfo, nil
}

func NewIdentityTokenInfo(creds *aws.Credentials, presignedURL string) (*AWSIdentityTokenInfo, error) {
	parsedURL, err := url.Parse(presignedURL)
	if err != nil {
		return nil, err
	}

	queryParams := parsedURL.Query()
	credentials := queryParams.Get(authCredentialKey)
	splitedCreds := strings.Split(credentials, "/")
	calculatedRegion := splitedCreds[2]
	identityTokenInfo := AWSIdentityTokenInfo{
		Method:             "POST",
		Host:               parsedURL.Host,
		Region:             calculatedRegion,
		Action:             authAction,
		Date:               queryParams.Get(authDateKey),
		ExpirationDuration: queryParams.Get(authExpiresKey),
		AccessKeyID:        creds.AccessKeyID,
		Signature:          queryParams.Get(authSignatureKey),
		SignedHeaders:      strings.Split(queryParams.Get(authSignedHeadersKey), ";"),
		Version:            queryParams.Get(authVersionKey),
		Algorithm:          queryParams.Get(authAlgorithmKey),
		SecurityToken:      queryParams.Get(authSecurityTokenKey),
	}
	return &identityTokenInfo, nil
}

func GetPresignedURL(ctx context.Context, params *IAMAuthParams, cfg *aws.Config, creds *aws.Credentials) (string, error) {
	stsClient := sts.NewFromConfig(*cfg)
	stsPresignClient := sts.NewPresignClient(stsClient, func(o *sts.PresignOptions) {
		o.ClientOptions = append(o.ClientOptions, func(opts *sts.Options) {
			opts.ClientLogMode = aws.LogSigning
		})
	})

	presign, err := stsPresignClient.PresignGetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{},
		sts.WithPresignClientFromClientOptions(sts.WithAPIOptions(SetHTTPHeaders(params.TokenRequestHeaders, params.URLPresignTTL))),
	)
	if err != nil {
		return "", err
	}
	return presign.URL, err
}

func GetCreds(ctx context.Context, cfg *aws.Config) (*aws.Credentials, error) {
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	if creds.Expired() {
		return nil, ErrAWSCredentialsExpired
	}
	return &creds, err
}

func SetHTTPHeaders(requestHeaders map[string]string, ttl time.Duration) func(*middleware.Stack) error {
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
				queryParams := req.URL.Query()
				queryParams.Set(authExpiresKey, fmt.Sprintf("%d", int(ttl.Seconds())))
				req.URL.RawQuery = queryParams.Encode()
			}
			return next.HandleBuild(ctx, in)
		}), middleware.Before)
	}
}
