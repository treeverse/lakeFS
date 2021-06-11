package nessie

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

// Test Developer Policies: AuthManageOwnCredentials, FSFullAccess, RepoManagementReadAll
func TestDeveloperPolicies(t *testing.T) {

	ctx, _, repo := setupTest(t)

	gid := "Developers"
	endpointURL := viper.GetString("endpoint_url") // defined in setup.go

	// generate the Developer client
	developerClient, err := NewClientFromGroup(t, ctx, gid, endpointURL)
	require.NoError(t, err, "failed to initialize client with group")

	// listing the available braches should succeed
	resBch, err := developerClient.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "Developer unexpectedly failed to list branches of repository")
	require.Equal(t, http.StatusOK, resBch.StatusCode(), "Developer unexpectedly failed to list branches of repository")

	branches := resBch.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCom, err := developerClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Developer unexpectedly failed to read branch commit")
	require.Equal(t, http.StatusOK, resCom.StatusCode(), "Developer unexpectedly failed to read branch commit")

	// creating a branch should be authorized
	branch1 := "feature-1"
	resAddBch, err := developerClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Developer failed while testing create branch")
	require.Equal(t, http.StatusCreated, resAddBch.StatusCode(), "Developer unexpectedly failed to create branch")

	// attempting to delete the repository should be authorized
	resDelRep, err := developerClient.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "Developer failed while testing delete repository")
	require.Equal(t, http.StatusUnauthorized, resDelRep.StatusCode(), "Developer unexpectedly did not receive unauthorized response while deleting repo")
}

// Test Super User Policies: AuthManageOwnCredentials, FSFullAccess, RepoManagementReadAll
func TestSuperUserPolicies(t *testing.T) {
	ctx, _, repo := setupTest(t)

	gid := "SuperUsers"
	endpointURL := viper.GetString("endpoint_url") // defined in setup.go

	// generate the SuperUser client
	superUserClient, err := NewClientFromGroup(t, ctx, gid, endpointURL)
	require.NoError(t, err, "failed to initialize client with group")

	// listing the available braches should succeed
	resBch, err := superUserClient.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "SuperUser unexpectedly failed to list branches of repository")
	require.Equal(t, http.StatusOK, resBch.StatusCode(), "SuperUser unexpectedly failed to list branches of repository")

	branches := resBch.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCom, err := superUserClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "SuperUser unexpectedly failed to read branch commit")
	require.Equal(t, http.StatusOK, resCom.StatusCode(), "SuperUser unexpectedly failed to read branch commit")

	// creating a branch should succeed
	branch1 := "feature-1"
	resAddBch, err := superUserClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "SuperUser failed while testing create branch")
	require.Equal(t, http.StatusCreated, resAddBch.StatusCode(), "SuperUser unexpectedly failed to create branch")

	// attempting to delete the repository should be authorized and result in no content response
	resDelRep, err := superUserClient.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "SuperUser failed while testing delete repository")
	require.Equal(t, http.StatusNoContent, resDelRep.StatusCode(), "SuperUser unexpectedly did not receive no content response while deleting repo")
}

// Test Viewer Policies: AuthManageOwnCredentials, FSReadAll
func TestViewerPolicies(t *testing.T) {
	ctx, _, repo := setupTest(t)

	gid := "Viewers"
	endpointURL := viper.GetString("endpoint_url") // defined in setup.go

	// generate the viewer client
	viewerClient, err := NewClientFromGroup(t, ctx, gid, endpointURL)
	require.NoError(t, err, "failed to initialize client with group")

	// listing the available branches should succeed
	resBch, err := viewerClient.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "Viewer unexpectedly failed to list branches of repository")
	require.Equal(t, http.StatusOK, resBch.StatusCode(), "Viewer unexpectedly failed to list branches of repository")

	branches := resBch.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCom, err := viewerClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Viewer unexpectedly failed to read branch commit")
	require.Equal(t, http.StatusOK, resCom.StatusCode(), "Viewer unexpectedly failed to read branch commit")

	// attempting to create a branch should be unauthorized
	branch1 := "feature-1"
	resAddBch, err := viewerClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Viewer failed while testing create branch")
	require.Equal(t, http.StatusUnauthorized, resAddBch.StatusCode(), "Viewer unexpectedly did not receive unauthorized response while creating branch")

	// attempting to delete the repository should be unauthorized
	resDelRep, err := viewerClient.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "Viewer failed while testing delete repository")
	require.Equal(t, http.StatusUnauthorized, resDelRep.StatusCode(), "Viewer unexpectedly did not receive unauthorized response while deleting repo")
}

// creates a client using the credentials of a user
func NewClientFromCreds(t *testing.T, creds *api.CredentialsWithSecret, endpointURL string) (*api.ClientWithResponses, error) {
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(creds.AccessKeyId, creds.SecretAccessKey)
	require.NoError(t, err, "could not initialize basic auth security provider")

	u, err := url.Parse(endpointURL)
	require.NoError(t, err, "Failed to parse endpoint URL "+endpointURL)

	if u.Path == "" || u.Path == "/" {
		endpointURL = strings.TrimRight(endpointURL, "/") + api.BaseURL
	}

	return api.NewClientWithResponses(endpointURL, api.WithRequestEditorFn(basicAuthProvider.Intercept))
}

// creates a client with a user of the given group
func NewClientFromGroup(t *testing.T, context context.Context, groupId string, endpointURL string) (*api.ClientWithResponses, error) {

	//create user and assign them the given group
	userId := "test-user-" + groupId
	_, err := client.CreateUserWithResponse(context, api.CreateUserJSONRequestBody{
		Id: userId,
	})
	require.NoError(t, err, "Failed to create user "+userId)

	_, err = client.AddGroupMembershipWithResponse(context, groupId, userId)
	require.NoError(t, err, "Failed to add group "+groupId+" to user "+userId)

	// give the user access credentials
	res, err := client.CreateCredentialsWithResponse(context, userId)
	require.NoError(t, err, "Failed to create credentials for user "+userId)
	require.Equal(t, http.StatusCreated, res.StatusCode(), "Failed to create credentials for user "+userId)

	viewerCredentials := res.JSON201

	return NewClientFromCreds(t, viewerCredentials, endpointURL)
}
