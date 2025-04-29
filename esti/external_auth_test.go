package esti

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
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

func TestAuthWithAwsIamRole(t *testing.T) {
	arnToAssume := "arn:aws:iam::977611293394:role/RoleToAssumeForHagridTests"
	principalID := "arn:aws:sts::977611293394:assumed-role/RoleToAssumeForHagridTests"
	// generate the SuperUser superClient
	ctx, log, _ := setupTest(t)
	if isBasicAuth(t, ctx) {
		t.Skip("Unsupported in basic auth configuration")
	}
	groups := []string{"Supers", "SuperUsers"}
	uid := "super"
	// map group names to IDs
	_, groupIDs := mapGroupNamesToIDs(t, ctx, groups)
	// generate the Super superClient
	superClient := newClientFromGroup(t, ctx, log, uid, groupIDs) // assume a role
	cfg, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err, "Error loading AWS config")
	stsClient := sts.NewFromConfig(cfg)

	sessionName := uuid.New().String()
	assumeRoleInput := &sts.AssumeRoleInput{
		RoleArn:         aws.String(arnToAssume),
		RoleSessionName: aws.String(sessionName),
	}

	assumeRoleOutput, err := stsClient.AssumeRole(ctx, assumeRoleInput)
	require.NoError(t, err, "Error assuming role")

	// create user external principal
	createUserExternalPrincipalResp, err := superClient.CreateUserExternalPrincipalWithResponse(ctx, uid, &apigen.CreateUserExternalPrincipalParams{PrincipalId: principalID}, apigen.CreateUserExternalPrincipalJSONRequestBody{})
	require.NoError(t, err, "Error creating user external principal")
	require.Equal(t, http.StatusCreated, createUserExternalPrincipalResp.StatusCode())

	// list user external principals
	listUserExternalPrincipalsResp, err := superClient.ListUserExternalPrincipalsWithResponse(ctx, uid, &apigen.ListUserExternalPrincipalsParams{})
	require.NoError(t, err, "Error listing user external principals")
	require.Equal(t, http.StatusOK, listUserExternalPrincipalsResp.StatusCode(), "Unexpected status code")
	require.Len(t, listUserExternalPrincipalsResp.JSON200.Results, 1, "Expected one external principal")
	require.Equal(t, principalID, listUserExternalPrincipalsResp.JSON200.Results[0].Id, "Unexpected principal ID")
	require.Equal(t, uid, listUserExternalPrincipalsResp.JSON200.Results[0].UserId, "Unexpected user ID")

	// login with external principal
	assumedStsClient := sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     *assumeRoleOutput.Credentials.AccessKeyId,
				SecretAccessKey: *assumeRoleOutput.Credentials.SecretAccessKey,
				SessionToken:    *assumeRoleOutput.Credentials.SessionToken,
			}, nil
		})
	})
	stsPresignClient := sts.NewPresignClient(assumedStsClient, func(o *sts.PresignOptions) {
		o.ClientOptions = append(o.ClientOptions, func(opts *sts.Options) {
			opts.ClientLogMode = aws.LogSigning
		})
	})
	presignGetCallerIdentityResp, err := stsPresignClient.PresignGetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{},
		sts.WithPresignClientFromClientOptions(sts.WithAPIOptions(setHTTPHeaders())),
	)

	require.NoError(t, err, "Error presign get caller identity")
	parsedURL, err := url.Parse(presignGetCallerIdentityResp.URL)
	require.NoError(t, err, "Error parsing presigned URL")
	queryParams := parsedURL.Query()
	credentials := queryParams.Get(AWSAuthCredentialKey)
	splitedCreds := strings.Split(credentials, "/")
	accessKeyID := splitedCreds[0]
	calculatedRegion := splitedCreds[2]
	identityTokenInfo := IdentityTokenInfo{
		Method:             "POST",
		Host:               parsedURL.Host,
		Region:             calculatedRegion,
		Action:             AWSAuthAction,
		Date:               queryParams.Get(AWSAuthDateKey),
		ExpirationDuration: queryParams.Get(AWSAuthExpiresKey),
		AccessKeyID:        accessKeyID,
		Signature:          queryParams.Get(AWSAuthSignatureKey),
		SignedHeaders:      strings.Split(queryParams.Get(AWSAuthSignedHeadersKey), ";"),
		Version:            queryParams.Get(AWSAuthVersionKey),
		Algorithm:          queryParams.Get(AWSAuthAlgorithmKey),
		SecurityToken:      queryParams.Get(AWSAuthSecurityTokenKey),
	}
	marshaledIdentityTokenInfo, err := json.Marshal(identityTokenInfo)
	require.NoError(t, err, "Error marshaling identity token info")
	encodedIdentityTokenInfo := base64.StdEncoding.EncodeToString(marshaledIdentityTokenInfo)
	externalPrincipalLoginResp, err := superClient.ExternalPrincipalLoginWithResponse(ctx, apigen.ExternalPrincipalLoginJSONRequestBody{IdentityRequest: map[string]interface{}{"identity_token": encodedIdentityTokenInfo}})
	require.NoError(t, err, "Error logging in with external principal")
	require.Equal(t, http.StatusOK, externalPrincipalLoginResp.StatusCode(), "Unexpected status code")

	// apiToken := externalPrincipalLoginResp.JSON200.Token
	// authProvider, err := securityprovider.NewSecurityProviderApiKey("header", "Authorization", "Bearer "+apiToken)
	// require.NoError(t, err, "Error creating security provider")
	// bearerClient, err := apigen.NewClientWithResponses(ParseEndpointURL(logger, viper.GetString("endpoint_url")), apigen.WithRequestEditorFn(authProvider.Intercept))
	// require.NoError(t, err, "Error creating superClient")
	// resp, err := bearerClient.ListRepositoriesWithResponse(ctx, apigen.ListRepositoriesParams{})
	// require.NoError(t, err, "Error listing repositories")
	// require.Equal(t, http.StatusOK, resp.StatusCode(), "Unexpected status code")

	// // delete user external principal
	// deleteUserExternalPrincipalResp, err := superClient.DeleteUserExternalPrincipalWithResponse(ctx, uid, apigen.DeleteUserExternalPrincipalParams{PrincipalId: principalID})
	// require.NoError(t, err, "Error deleting user external principal")
	// require.Equal(t, http.StatusNoContent, deleteUserExternalPrincipalResp.StatusCode(), "Unexpected status code")

	// // list user external principals
	// listUserExternalPrincipalsResp, err = superClient.ListUserExternalPrincipalsWithResponse(ctx, uid, apigen.ListUserExternalPrincipalsParams{})
	// require.NoError(t, err, "Error listing user external principals")
	// require.Equal(t, http.StatusOK, listUserExternalPrincipalsResp.StatusCode(), "Unexpected status code")
	// require.Len(t, listUserExternalPrincipalsResp.JSON200.Results, 0, "Expected no external principals")
}

func setHTTPHeaders() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("IDoEVaultGrant", func(
			ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
		) (
			middleware.BuildOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.Method = "POST"
				req.Header.Add("x-lakefs-server-id", "hagrid")
				queryParams := req.Request.URL.Query()
				queryParams.Set(AWSAuthExpiresKey, "60")
				req.Request.URL.RawQuery = queryParams.Encode()
			}
			return next.HandleBuild(ctx, in)
		}), middleware.Before)
	}
}

func ParseEndpointURL(logger logging.Logger, endpointURL string) string {
	u, err := url.Parse(endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client with security provider")
	}
	if u.Path == "" || u.Path == "/" {
		// apiutil.BaseURL is actually the base API path
		endpointURL = strings.TrimRight(endpointURL, "/") + apiutil.BaseURL
	}

	return endpointURL
}
