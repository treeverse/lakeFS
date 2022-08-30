package esti

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

// Test Admin Policies: AuthFullAccess, ExportSetConfiguration, FSFullAccess, RepoManagementFullAccess
func TestAdminPolicies(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	adminClient := client

	// creating new group should succeed
	gid := "TestGroup"
	resCreateGroup, err := adminClient.CreateGroupWithResponse(ctx, api.CreateGroupJSONRequestBody{
		Id: gid,
	})
	require.NoError(t, err, "Admin failed while creating group")
	require.Equal(t, http.StatusCreated, resCreateGroup.StatusCode(), "Admin unexpectedly failed to create group")

	// adding policies to the group should succeed
	pid := "TestPolicy"
	resCreatePolicy, err := adminClient.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
		CreationDate: api.Int64Ptr(time.Now().Unix()),
		Id:           pid,
		Statement: []api.Statement{
			{
				Action:   []string{"fs:ReadObject"},
				Effect:   "allow",
				Resource: "arn:lakefs:fs:::repository/foo/object/*",
			},
		},
	})
	require.NoError(t, err, "Admin failed while creating policy")
	require.Equal(t, http.StatusCreated, resCreatePolicy.StatusCode(), "Admin unexpectedly failed to create policy")

	resAddPolicy, err := adminClient.AttachPolicyToGroupWithResponse(ctx, gid, pid)
	require.NoError(t, err, "Admin failed while adding policy to group")
	require.Equal(t, http.StatusCreated, resAddPolicy.StatusCode(), "Admin unexpectedly failed to add policy to group")

	// creating a new user should succeed
	uid := "test-user"
	resCreateUser, err := adminClient.CreateUserWithResponse(ctx, api.CreateUserJSONRequestBody{
		Id: uid,
	})
	require.NoError(t, err, "Admin failed while creating user")
	require.Equal(t, http.StatusCreated, resCreateUser.StatusCode(), "Admin unexpectedly failed to create user")

	// adding group to user should succeed
	resAddGroup, err := adminClient.AddGroupMembershipWithResponse(ctx, gid, uid)
	require.NoError(t, err, "Admin failed while adding the group membership to the user")
	require.Equal(t, http.StatusCreated, resAddGroup.StatusCode(), "Admin unexpectedly failed to add the group membership to the user")

	// deleting the user should succeed
	resDeleteUser, err := adminClient.DeleteUserWithResponse(ctx, uid)
	require.NoError(t, err, "Admin failed while deleting the user")
	require.Equal(t, http.StatusNoContent, resDeleteUser.StatusCode(), "Admin unexpectedly failed to delete the user")
}

// Test Super User Policies: AuthManageOwnCredentials, FSFullAccess, RepoManagementReadAll
func TestSuperUserPolicies(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	gid := "SuperUsers"

	// generate the SuperUser client
	superUserClient := newClientFromGroup(t, ctx, logger, gid)

	// listing the available braches should succeed
	resListBranches, err := superUserClient.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "SuperUser unexpectedly failed while listing branches of repository")
	require.Equal(t, http.StatusOK, resListBranches.StatusCode(), "SuperUser unexpectedly failed to list branches of repository")

	branches := resListBranches.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCommit, err := superUserClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "SuperUser unexpectedly failed while reading branch commit")
	require.Equal(t, http.StatusOK, resCommit.StatusCode(), "SuperUser unexpectedly failed to read branch commit")

	// creating a branch should succeed
	branch1 := "feature-1"
	resAddBranch, err := superUserClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "SuperUser unexpectedly failed while testing create branch")
	require.Equal(t, http.StatusCreated, resAddBranch.StatusCode(), "SuperUser unexpectedly failed to create branch")

	// merging a branch should succeed
	resMerge, err := mergeAuthTest(t, superUserClient, ctx, repo, branch1)
	require.NoError(t, err, "SuperUser failed while merging branches")
	require.Equal(t, http.StatusOK, resMerge.StatusCode(), "SuperUser unexpectedly failed to merge branch")

	// deleting the repository should succeed and result in no content response
	resDeleteRepo, err := superUserClient.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "SuperUser failed while testing delete repository")
	require.Equal(t, http.StatusNoContent, resDeleteRepo.StatusCode(), "SuperUser unexpectedly did not receive \"no content\" response while deleting repo")

	// attempting to list the users should be unauthorized
	resListUsers, err := superUserClient.ListUsersWithResponse(ctx, &api.ListUsersParams{})
	require.NoError(t, err, "SuperUser failed while testing list users")
	require.Equal(t, http.StatusUnauthorized, resListUsers.StatusCode(), "SuperUser unexpectedly did not receive unauthorized response while listing users")

	// attempting to list the group policies should be unauthorized
	resListPolicies, err := superUserClient.ListGroupPoliciesWithResponse(ctx, "Admins", &api.ListGroupPoliciesParams{})
	require.NoError(t, err, "SuperUser failed while testing list Admin policies")
	require.Equal(t, http.StatusUnauthorized, resListPolicies.StatusCode(), "SuperUser unexpectedly did not receive unauthorized response while listing Admin policies")
}

// Test Developer Policies: AuthManageOwnCredentials, FSFullAccess, RepoManagementReadAll
func TestDeveloperPolicies(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	gid := "Developers"

	// generate the Developer client
	developerClient := newClientFromGroup(t, ctx, logger, gid)

	// listing the available braches should succeed
	resListBranches, err := developerClient.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "Developer failed while listing branches of repository")
	require.Equal(t, http.StatusOK, resListBranches.StatusCode(), "Developer unexpectedly failed to list branches of repository")

	branches := resListBranches.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCommit, err := developerClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Developer failed while reading branch commit")
	require.Equal(t, http.StatusOK, resCommit.StatusCode(), "Developer unexpectedly failed to read branch commit")

	// creating a branch should succeed
	branch1 := "feature-1"
	resAddBranch, err := developerClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Developer failed while testing create branch")
	require.Equal(t, http.StatusCreated, resAddBranch.StatusCode(), "Developer unexpectedly failed to create branch")

	// merging a branch should succeed
	resMerge, err := mergeAuthTest(t, developerClient, ctx, repo, branch1)
	require.NoError(t, err, "Developer failed while merging branches")
	require.Equal(t, http.StatusOK, resMerge.StatusCode(), "Developer unexpectedly failed to merge branch")

	// attempting to delete the repository should be unauthorized
	resDeleteRepo, err := developerClient.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "Developer failed while testing delete repository")
	require.Equal(t, http.StatusUnauthorized, resDeleteRepo.StatusCode(), "Developer unexpectedly did not receive unauthorized response while deleting repo")

	// attempting to list the users should be unauthorized
	resListUsers, err := developerClient.ListUsersWithResponse(ctx, &api.ListUsersParams{})
	require.NoError(t, err, "Developer failed while testing list users")
	require.Equal(t, http.StatusUnauthorized, resListUsers.StatusCode(), "Developer unexpectedly did not receive unauthorized response while listing users")
}

// Test Viewer Policies: AuthManageOwnCredentials, FSReadAll
func TestViewerPolicies(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	gid := "Viewers"

	// generate the viewer client
	viewerClient := newClientFromGroup(t, ctx, logger, gid)

	// listing the available branches should succeed
	resListBranches, err := viewerClient.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "Viewer failed while listing branches of repository")
	require.Equal(t, http.StatusOK, resListBranches.StatusCode(), "Viewer unexpectedly failed to list branches of repository")

	branches := resListBranches.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCommit, err := viewerClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Viewer failed while reading branch commit")
	require.Equal(t, http.StatusOK, resCommit.StatusCode(), "Viewer unexpectedly failed to read branch commit")

	// attempting to create a branch should be unauthorized
	branch1 := "feature-1"
	resAddBranch, err := viewerClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Viewer failed while testing create branch")
	require.Equal(t, http.StatusUnauthorized, resAddBranch.StatusCode(), "Viewer unexpectedly did not receive unauthorized response while creating branch")

	// attempting to delete the repository should be unauthorized
	resDeleteRepo, err := viewerClient.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "Viewer failed while testing delete repository")
	require.Equal(t, http.StatusUnauthorized, resDeleteRepo.StatusCode(), "Viewer unexpectedly did not receive unauthorized response while deleting repo")
}

// Creates a client with a user of the given group
func newClientFromGroup(t *testing.T, context context.Context, logger logging.Logger, groupId string) *api.ClientWithResponses {
	userId := "test-user-" + groupId
	endpointURL := testutil.ParseEndpointURL(logger, viper.GetString("endpoint_url")) // defined in setup.go

	adminClient := client
	_, err := adminClient.CreateUserWithResponse(context, api.CreateUserJSONRequestBody{
		Id: userId,
	})
	require.NoError(t, err, "Failed to create user "+userId)

	_, err = adminClient.AddGroupMembershipWithResponse(context, groupId, userId)
	require.NoError(t, err, "Failed to add group "+groupId+" to user "+userId)

	// give the user access credentials
	r, err := adminClient.CreateCredentialsWithResponse(context, userId)
	require.NoError(t, err, "Failed to create credentials for user "+userId)
	require.Equal(t, http.StatusCreated, r.StatusCode(), "Failed to create credentials for user "+userId)

	viewerCredentials := r.JSON201

	// create the new client
	cli, err := testutil.NewClientFromCreds(logger, viewerCredentials.AccessKeyId, viewerCredentials.SecretAccessKey, endpointURL)
	require.NoError(t, err, "failed to initialize client with group")

	return cli
}

// Tests merge with different clients
func mergeAuthTest(t *testing.T, cli *api.ClientWithResponses, ctx context.Context, repo string, branch string) (*api.MergeIntoBranchResponse, error) {
	uploadFileRandomData(ctx, t, repo, mainBranch, "README", false)

	resMainCommit, err := cli.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Initial content"})
	require.NoError(t, err, "failed to commit initial content in merge auth test")
	require.Equal(t, http.StatusCreated, resMainCommit.StatusCode())

	uploadFileRandomData(ctx, t, repo, branch, "foo.txt", false)

	resBranchCommit, err := cli.CommitWithResponse(ctx, repo, branch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Additional content"})
	require.NoError(t, err, "failed to commit additional content in merge auth test")
	require.Equal(t, http.StatusCreated, resBranchCommit.StatusCode())

	return client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, api.MergeIntoBranchJSONRequestBody{})
}
