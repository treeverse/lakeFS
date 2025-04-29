package esti

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	authentication "github.com/treeverse/lakefs/pkg/authentication/externalidp"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestAuthWithAwsIamRole(t *testing.T) {
	principalID := "arn:aws:sts::977611293394:assumed-role/RoleToAssumeForHagridTests"
	// generate the SuperUser superClient
	ctx, log, _ := setupTest(t)
	endpointURL := testutil.ParseEndpointURL(logger, viper.GetString("endpoint_url")) // defined in setup.go

	if isBasicAuth(t, ctx) {
		t.Skip("Unsupported in basic auth configuration")
	}
	params := authentication.AWSIAMParams{
		ProviderType:        "aws_iam",
		TokenRequestHeaders: map[string]string{"x-lakefs-server-id": "esti"},
		TokenTTL:            100 * time.Second,
		URLPresignTTL:       100 * time.Second,
	}
	groups := []string{"SuperUsers"}
	uid := "super"
	// map group names to IDs
	_, groupIDs := mapGroupNamesToIDs(t, ctx, groups)
	// generate the Super superClient
	superClient := newClientFromGroup(t, ctx, log, uid, groupIDs) // assume a role
	provider := authentication.NewAWSProvider(params, endpointURL, &http.Client{})
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		fmt.Println("credsssss\n\n", creds.AccessKeyID, creds.SecretAccessKey, creds.Expires)
	}
	fmt.Println()
	require.NoError(t, err, "Error loading AWS config")

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

	externalPrincipalLoginResp, err := provider.Login()
	if err != nil {
		fmt.Println(externalPrincipalLoginResp.Token)
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
