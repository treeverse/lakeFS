package awsiam

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
)

const (
	AuthVersion       = "2011-06-15"
	AuthMethod        = http.MethodPost
	AuthAction        = "GetCallerIdentity"
	AuthAlgorithm     = "AWS4-HMAC-SHA256"
	StsGlobalEndpoint = "sts.amazonaws.com"
	AuthActionKey     = "Action"
	AuthVersionKey    = "Version"
	AuthAlgorithmKey  = "X-Amz-Algorithm"
	//nolint:gosec
	AuthCredentialKey = "X-Amz-Credential"
	AuthDateKey       = "X-Amz-Date"
	AuthExpiresKey    = "X-Amz-Expires"
	//nolint:gosec
	AuthSecurityTokenKey  = "X-Amz-Security-Token"
	AuthSignedHeadersKey  = "X-Amz-SignedHeaders"
	AuthSignatureKey      = "X-Amz-Signature"
	DatetimeFormat        = "20060102T150405Z"
	CredentialTimeFormat  = "20060102"
	DefaultSTSLoginExpire = 15 * time.Minute
)

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

type IAMAuthParams struct {
	ProviderType        string
	TokenRequestHeaders map[string]string
	URLPresignTTL       time.Duration
	TokenTTL            time.Duration
	RefreshInterval     time.Duration
}

func GenerateIdentityTokenInfo(ctx context.Context, params *IAMAuthParams, stsClient *sts.Client) (*AWSIdentityTokenInfo, error) {
	url, err := PresignGetCallerIdentityFromAuthParams(ctx, params, stsClient)
	if err != nil {
		return nil, fmt.Errorf("generating sts presigned url: %w", err)
	}
	tokenInfo, err := ParsePresignedURL(url)
	if err != nil {
		return nil, fmt.Errorf("parsing credentials sts presigned url: %w", err)
	}
	return tokenInfo, nil
}

func ParsePresignedURL(presignedURL string) (*AWSIdentityTokenInfo, error) {
	parsedURL, err := url.Parse(presignedURL)
	if err != nil {
		return nil, err
	}

	queryParams := parsedURL.Query()
	credentials := queryParams.Get(AuthCredentialKey)
	splitedCreds := strings.Split(credentials, "/")
	if len(splitedCreds) < 3 {
		return nil, fmt.Errorf("invalid credentials format: %s", credentials)
	}
	calculatedRegion := splitedCreds[2]
	return &AWSIdentityTokenInfo{
		Method:             "POST",
		Host:               parsedURL.Host,
		Region:             calculatedRegion,
		Action:             AuthAction,
		Date:               queryParams.Get(AuthDateKey),
		ExpirationDuration: queryParams.Get(AuthExpiresKey),
		AccessKeyID:        splitedCreds[0],
		Signature:          queryParams.Get(AuthSignatureKey),
		SignedHeaders:      strings.Split(queryParams.Get(AuthSignedHeadersKey), ";"),
		Version:            queryParams.Get(AuthVersionKey),
		Algorithm:          queryParams.Get(AuthAlgorithmKey),
		SecurityToken:      queryParams.Get(AuthSecurityTokenKey),
	}, nil
}

func PresignGetCallerIdentityFromAuthParams(ctx context.Context, params *IAMAuthParams, stsClient *sts.Client) (string, error) {
	setHTTPHeaders := func(requestHeaders map[string]string, ttl time.Duration) func(*middleware.Stack) error {
		middlewareName := "AddHeaders" + uuid.New().String()
		return func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc(middlewareName, func(
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
					queryParams.Set(AuthExpiresKey, fmt.Sprintf("%d", int(ttl.Seconds())))
					req.URL.RawQuery = queryParams.Encode()
				}
				return next.HandleBuild(ctx, in)
			}), middleware.Before)
		}
	}

	stsPresignClient := sts.NewPresignClient(stsClient, func(o *sts.PresignOptions) {
		o.ClientOptions = append(o.ClientOptions, func(opts *sts.Options) {
			opts.ClientLogMode = aws.LogSigning
		})
	})

	presign, err := stsPresignClient.PresignGetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{},
		sts.WithPresignClientFromClientOptions(sts.WithAPIOptions(setHTTPHeaders(params.TokenRequestHeaders, params.URLPresignTTL))),
	)
	if err != nil {
		return "", err
	}
	return presign.URL, err
}

func RetrieveCredentials(ctx context.Context, cfg *aws.Config) (*aws.Credentials, error) {
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	return &creds, err
}
