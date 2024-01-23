package esti

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/exp/slices"
)

// Test Admin permissions: AuthFullAccess, ExportSetConfiguration, FSFullAccess, RepoManagementFullAccess
func TestAdminPermissions(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// creating a new group should succeed
	const gname = "TestGroup"
	resCreateGroup, err := client.CreateGroupWithResponse(ctx, apigen.CreateGroupJSONRequestBody{
		Id: gname,
	})
	require.NoError(t, err, "Admin failed while creating group")
	require.Equal(t, http.StatusCreated, resCreateGroup.StatusCode(), "Admin unexpectedly failed to create group")
	groupID := resCreateGroup.JSON201.Id

	// setting a group ACL should succeed
	resSetACL, err := client.SetGroupACLWithResponse(ctx, groupID, apigen.SetGroupACLJSONRequestBody{
		Permission: "Write",
	})
	require.NoError(t, err, "Admin failed while setting group ACL")
	require.Equal(t, http.StatusCreated, resSetACL.StatusCode(), "Admin unexpectedly failed to set group ACL")

	// creating a new user should succeed
	const uid = "test-user"
	resCreateUser, err := client.CreateUserWithResponse(ctx, apigen.CreateUserJSONRequestBody{
		Id: uid,
	})
	require.NoError(t, err, "Admin failed while creating user")
	require.Equal(t, http.StatusCreated, resCreateUser.StatusCode(), "Admin unexpectedly failed to create user")

	// adding group to user should succeed
	resAddGroup, err := client.AddGroupMembershipWithResponse(ctx, groupID, uid)
	require.NoError(t, err, "Admin failed while adding the group membership to the user")
	require.Equal(t, http.StatusCreated, resAddGroup.StatusCode(), "Admin unexpectedly failed to add the group membership to the user")

	// deleting the user should succeed
	resDeleteUser, err := client.DeleteUserWithResponse(ctx, uid)
	require.NoError(t, err, "Admin failed while deleting the user")
	require.Equal(t, http.StatusNoContent, resDeleteUser.StatusCode(), "Admin unexpectedly failed to delete the user")
}

// Test Super Permissions: AuthManageOwnCredentials, FSFullAccess, RepoManagementReadAll
func TestSuperPermissions(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	groups := []string{"Supers", "SuperUsers"}

	// map group names to IDs
	mapGroupNameToID, groupIDs := mapGroupNamesToIDs(t, ctx, groups)
	// generate the Super client
	superClient := newClientFromGroup(t, ctx, logger, "super", groupIDs)

	// listing the available branches should succeed
	resListBranches, err := superClient.ListBranchesWithResponse(ctx, repo, &apigen.ListBranchesParams{})
	require.NoError(t, err, "Super unexpectedly failed while listing branches of repository")
	require.Equal(t, http.StatusOK, resListBranches.StatusCode(), "Super unexpectedly failed to list branches of repository")

	branches := resListBranches.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCommit, err := superClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Super unexpectedly failed while reading branch commit")
	require.Equal(t, http.StatusOK, resCommit.StatusCode(), "Super unexpectedly failed to read branch commit")

	// creating a branch should succeed
	branch1 := "feature-1"
	resAddBranch, err := superClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Super unexpectedly failed while testing create branch")
	require.Equal(t, http.StatusCreated, resAddBranch.StatusCode(), "Super unexpectedly failed to create branch")

	// merging a branch should succeed
	resMerge, err := mergeAuthTest(t, superClient, ctx, repo, branch1)
	require.NoError(t, err, "Super failed while merging branches")
	require.Equal(t, http.StatusOK, resMerge.StatusCode(), "Super unexpectedly failed to merge branch")

	// deleting the repository should succeed and result in no content response
	resDeleteRepo, err := superClient.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
	require.NoError(t, err, "Super failed while testing delete repository")
	require.Equal(t, http.StatusNoContent, resDeleteRepo.StatusCode(), "Super unexpectedly did not receive \"no content\" response while deleting repo")

	// attempting to list the users should be unauthorized
	resListUsers, err := superClient.ListUsersWithResponse(ctx, &apigen.ListUsersParams{})
	require.NoError(t, err, "Super failed while testing list users")
	require.Equal(t, http.StatusUnauthorized, resListUsers.StatusCode(), "Super unexpectedly did not receive unauthorized response while listing users")

	// attempting to get the group ACL should be unauthorized
	resGetGroupACL, err := superClient.GetGroupACLWithResponse(ctx, mapGroupNameToID["Admins"])
	require.NoError(t, err, "Super failed while testing get Admins ACL")
	require.Equal(t, http.StatusUnauthorized, resGetGroupACL.StatusCode(), "Super unexpectedly did not receive unauthorized response while getting Admins ACL")
}

// Test Writer Permissions: AuthManageOwnCredentials, FSFullAccess, RepoManagementReadAll
func TestWriterPermissions(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	groups := []string{"Writers", "Developers"}
	// map group names to IDs
	_, groupIDs := mapGroupNamesToIDs(t, ctx, groups)

	// generate the Writer client
	writerClient := newClientFromGroup(t, ctx, logger, "writer", groupIDs)

	// listing the available branches should succeed
	resListBranches, err := writerClient.ListBranchesWithResponse(ctx, repo, &apigen.ListBranchesParams{})
	require.NoError(t, err, "Writer failed while listing branches of repository")
	require.Equal(t, http.StatusOK, resListBranches.StatusCode(), "Writer unexpectedly failed to list branches of repository")

	branches := resListBranches.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCommit, err := writerClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Writer failed while reading branch commit")
	require.Equal(t, http.StatusOK, resCommit.StatusCode(), "Writer unexpectedly failed to read branch commit")

	// creating a branch should succeed
	branch1 := "feature-1"
	resAddBranch, err := writerClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Writer failed while testing create branch")
	require.Equal(t, http.StatusCreated, resAddBranch.StatusCode(), "Writer unexpectedly failed to create branch")

	// merging a branch should succeed
	resMerge, err := mergeAuthTest(t, writerClient, ctx, repo, branch1)
	require.NoError(t, err, "Writer failed while merging branches")
	require.Equal(t, http.StatusOK, resMerge.StatusCode(), "Writer unexpectedly failed to merge branch")

	// attempting to delete the repository should be unauthorized
	resDeleteRepo, err := writerClient.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
	require.NoError(t, err, "Writer failed while testing delete repository")
	require.Equal(t, http.StatusUnauthorized, resDeleteRepo.StatusCode(), "Writer unexpectedly did not receive unauthorized response while deleting repo")

	// attempting to list the users should be unauthorized
	resListUsers, err := writerClient.ListUsersWithResponse(ctx, &apigen.ListUsersParams{})
	require.NoError(t, err, "Writer failed while testing list users")
	require.Equal(t, http.StatusUnauthorized, resListUsers.StatusCode(), "Writer unexpectedly did not receive unauthorized response while listing users")
}

// Test Reader Permissions: AuthManageOwnCredentials, FSReadAll
func TestReaderPermissions(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	groups := []string{"Readers", "Viewers"}

	// map group names to IDs
	_, groupIDs := mapGroupNamesToIDs(t, ctx, groups)

	// generate the reader client
	readerClient := newClientFromGroup(t, ctx, logger, "reader", groupIDs)

	// listing the available branches should succeed
	resListBranches, err := readerClient.ListBranchesWithResponse(ctx, repo, &apigen.ListBranchesParams{})
	require.NoError(t, err, "Reader failed while listing branches of repository")
	require.Equal(t, http.StatusOK, resListBranches.StatusCode(), "Reader unexpectedly failed to list branches of repository")

	branches := resListBranches.JSON200.Results

	// reading the commit of the main branch of the repo should succeed
	resCommit, err := readerClient.GetCommitWithResponse(ctx, repo, branches[0].CommitId)
	require.NoError(t, err, "Reader failed while reading branch commit")
	require.Equal(t, http.StatusOK, resCommit.StatusCode(), "Reader unexpectedly failed to read branch commit")

	// attempting to create a branch should be unauthorized
	const branch1 = "feature-1"
	resAddBranch, err := readerClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch1,
		Source: mainBranch,
	})
	require.NoError(t, err, "Reader failed while testing create branch")
	require.Equal(t, http.StatusUnauthorized, resAddBranch.StatusCode(), "Reader unexpectedly did not receive unauthorized response while creating branch")

	// attempting to delete the repository should be unauthorized
	resDeleteRepo, err := readerClient.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
	require.NoError(t, err, "Reader failed while testing delete repository")
	require.Equal(t, http.StatusUnauthorized, resDeleteRepo.StatusCode(), "Reader unexpectedly did not receive unauthorized response while deleting repo")
}

// Creates a client with a user of the given group
func newClientFromGroup(t *testing.T, context context.Context, logger logging.Logger, id string, groupIDs []string) *apigen.ClientWithResponses {
	endpointURL := testutil.ParseEndpointURL(logger, viper.GetString("endpoint_url")) // defined in setup.go

	userID := "test-user-" + id
	_, err := client.CreateUserWithResponse(context, apigen.CreateUserJSONRequestBody{
		Id: userID,
	})
	require.NoErrorf(t, err, "Failed to create user %s", userID)

	addGroupStatusCodes := make([]int, len(groupIDs))
	for i, groupID := range groupIDs {
		resp, err := client.AddGroupMembershipWithResponse(context, groupID, userID)
		require.NoErrorf(t, err, "Failed to add group %s to user %s", groupID, userID)
		addGroupStatusCodes[i] = resp.StatusCode()
	}
	require.Containsf(t, addGroupStatusCodes, http.StatusCreated, "Failed to add group membership to user %s", userID)

	// give the user access credentials
	r, err := client.CreateCredentialsWithResponse(context, userID)
	require.NoErrorf(t, err, "Failed to create credentials for user %s", userID)
	require.Equalf(t, http.StatusCreated, r.StatusCode(), "Failed to create credentials for user %s", userID)

	// create the new client
	cli, err := testutil.NewClientFromCreds(logger, r.JSON201.AccessKeyId, r.JSON201.SecretAccessKey, endpointURL)
	require.NoError(t, err, "failed to initialize client with group")

	return cli
}

// Tests merge with different clients
func mergeAuthTest(t *testing.T, cli *apigen.ClientWithResponses, ctx context.Context, repo string, branch string) (*apigen.MergeIntoBranchResponse, error) {
	uploadFileRandomData(ctx, t, repo, mainBranch, "README")

	resMainCommit, err := cli.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: "Initial content"})
	require.NoError(t, err, "failed to commit initial content in merge auth test")
	require.Equal(t, http.StatusCreated, resMainCommit.StatusCode())

	uploadFileRandomData(ctx, t, repo, branch, "foo.txt")

	resBranchCommit, err := cli.CommitWithResponse(ctx, repo, branch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: "Additional content"})
	require.NoError(t, err, "failed to commit additional content in merge auth test")
	require.Equal(t, http.StatusCreated, resBranchCommit.StatusCode())

	return client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, apigen.MergeIntoBranchJSONRequestBody{})
}

func mapGroupNamesToIDs(t *testing.T, ctx context.Context, groups []string) (map[string]string, []string) {
	groupIDs := make([]string, len(groups))
	mapGroupNameToID := make(map[string]string)

	// get group list
	resListGroups, err := client.ListGroupsWithResponse(ctx, &apigen.ListGroupsParams{Amount: apiutil.Ptr(apigen.PaginationAmount(-1))})
	require.NoError(t, err, "unexpectedly failed while listing groups")
	require.NotNil(t, resListGroups.JSON200, "unexpectedly got empty response when listing groups")
	for _, group := range resListGroups.JSON200.Results {
		grpName := swag.StringValue(group.Name)
		if grpName != "" {
			mapGroupNameToID[grpName] = group.Id
			if slices.Contains(groups, grpName) {
				groupIDs = append(groupIDs, group.Id)
			}
		}
	}
	return mapGroupNameToID, groupIDs
}
