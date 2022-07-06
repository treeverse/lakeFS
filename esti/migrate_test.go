package esti

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

// state to be used
var state migrateTestState

type migrateTestState struct {
	Multiparts multipartsState
	Actions    actionsState
	Auth       authState
}

type multipartsState struct {
	Repo           string                         `json:"repo"`
	Info           s3.CreateMultipartUploadOutput `json:"info"`
	CompletedParts []*s3.CompletedPart            `json:"completed_parts"`
	Content        string                         `json:"content"`
}

type actionRun struct {
	Run   api.ActionRun `json:"run"`
	Hooks []api.HookRun `json:"hooks"`
}

type actionsState struct {
	Repo        string       `json:"repo"`
	WebhookPort int          `json:"webhook_port"`
	Runs        []*actionRun `json:"runs"`
}

type authCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

type authUser struct {
	Credentials *authCredentials `json:"credentials"`
}

type authState struct {
	Repo          string   `json:"repo"`
	ViewerUser    authUser `json:"viewer_user"`
	DeveloperUser authUser `json:"developer_user"`
	SuperUser     authUser `json:"super_user"`
	AdminUser     authUser `json:"admin_user"`
	CustomUser    authUser `json:"custom_user"`
}

type userPermissions struct {
	canCreateUser   bool
	canListUsers    bool
	canListRepos    bool
	canReadRepo     bool
	canCreateBranch bool
	canDeleteBranch bool
}

const (
	migrateMultipartsFile     = "multipart_file"
	migrateMultipartsFilepath = mainBranch + "/" + migrateMultipartsFile
	migrateStateRepoName      = "migrate"
	migrateStateObjectPath    = "state.json"
	migratePrePartsCount      = 3
	migratePostPartsCount     = 2
	authCustomGroupName       = "user-defined-group"
)

var (
	viewerPermissions = &userPermissions{
		canCreateUser:   false,
		canListUsers:    false,
		canListRepos:    true,
		canReadRepo:     true,
		canCreateBranch: false,
		canDeleteBranch: false,
	}

	developerPermissions = &userPermissions{
		canCreateUser:   false,
		canListUsers:    false,
		canListRepos:    true,
		canReadRepo:     true,
		canCreateBranch: true,
		canDeleteBranch: true,
	}

	superUserPermissions = &userPermissions{
		canCreateUser:   false,
		canListUsers:    false,
		canListRepos:    true,
		canReadRepo:     true,
		canCreateBranch: true,
		canDeleteBranch: true,
	}

	adminPermissions = &userPermissions{
		canCreateUser:   true,
		canListUsers:    true,
		canListRepos:    true,
		canReadRepo:     true,
		canCreateBranch: true,
		canDeleteBranch: true,
	}

	customPermissions = &userPermissions{
		canCreateUser:   false,
		canListUsers:    false,
		canListRepos:    true,
		canReadRepo:     false,
		canCreateBranch: false,
		canDeleteBranch: false,
	}
)

func TestMigrate(t *testing.T) {
	postMigrate := viper.GetViper().GetBool("post_migrate")

	if postMigrate {
		postMigrateTests(t)
	} else {
		preMigrateTests(t)
	}
}

func preMigrateTests(t *testing.T) {
	// all pre tests execution
	t.Run("TestPreMigrateMultipart", testPreMigrateMultipart)
	t.Run("TestPreMigrateActions", testPreMigrateActions)
	t.Run("TestPreMigrateAuth", testPreMigrateAuth)

	saveStateInLakeFS(t)
}

func postMigrateTests(t *testing.T) {
	readStateFromLakeFS(t)

	// all post tests execution
	t.Run("TestPostMigrateMultipart", testPostMigrateMultipart)
	t.Run("TestPostMigrateActions", testPostMigrateActions)
	t.Run("TestPostMigrateAuth", testPostMigrateAuth)
}

func saveStateInLakeFS(t *testing.T) {
	// write the state file
	stateBytes, err := json.Marshal(&state)
	require.NoError(t, err, "marshal state")

	ctx := context.Background()
	_ = createRepositoryByName(ctx, t, migrateStateRepoName)

	// file is big - so we better use multipart writing here.
	resp, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(migrateStateRepoName),
		Key:    aws.String("main/" + migrateStateObjectPath),
	})
	require.NoError(t, err, "failed to create multipart upload for state.json")

	var parts [][]byte
	for i := 0; i < len(stateBytes); i += multipartPartSize {
		last := i + multipartPartSize
		if last > len(stateBytes) {
			last = len(stateBytes)
		}
		parts = append(parts, stateBytes[i:last])
	}
	completedParts := uploadMultipartParts(t, logger, resp, parts, 0)
	_, err = uploadMultipartComplete(svc, resp, completedParts)
	require.NoError(t, err, "writing state file")
}

func readStateFromLakeFS(t *testing.T) {
	// read the state file
	ctx := context.Background()
	resp, err := client.GetObjectWithResponse(ctx, migrateStateRepoName, "main", &api.GetObjectParams{Path: migrateStateObjectPath})
	require.NoError(t, err, "reading state file")
	require.Equal(t, http.StatusOK, resp.StatusCode())

	err = json.Unmarshal(resp.Body, &state)
	require.NoError(t, err, "unmarshal state from response")
}

// Multiparts Upload Tests

func testPreMigrateMultipart(t *testing.T) {
	_, logger, repo := setupTest(t)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(repo),
		Key:    aws.String(migrateMultipartsFilepath),
	}

	resp, err := svc.CreateMultipartUpload(input)
	require.NoError(t, err, "failed to create multipart upload")
	logger.Info("Created multipart upload request")

	partsConcat, completedParts := createAndUploadParts(t, logger, resp, migratePrePartsCount, 0)

	state.Multiparts.Repo = repo
	state.Multiparts.Info = *resp
	state.Multiparts.CompletedParts = completedParts
	state.Multiparts.Content = base64.StdEncoding.EncodeToString(partsConcat)
}

func createAndUploadParts(t *testing.T, logger logging.Logger, resp *s3.CreateMultipartUploadOutput, count, firstIndex int) ([]byte, []*s3.CompletedPart) {
	parts := make([][]byte, count)
	var partsConcat []byte
	for i := 0; i < count; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i + firstIndex)
		partsConcat = append(partsConcat, parts[i]...)
	}

	completedParts := uploadMultipartParts(t, logger, resp, parts, firstIndex)
	return partsConcat, completedParts
}

func testPostMigrateMultipart(t *testing.T) {
	ctx := context.Background()

	partsConcat, completedParts := createAndUploadParts(t, logger, &state.Multiparts.Info, migratePostPartsCount, migratePrePartsCount)

	completeResponse, err := uploadMultipartComplete(svc, &state.Multiparts.Info, append(state.Multiparts.CompletedParts, completedParts...))
	require.NoError(t, err, "failed to complete multipart upload")

	logger.WithField("key", completeResponse.Key).Info("Completed multipart request successfully")

	getResp, err := client.GetObjectWithResponse(ctx, state.Multiparts.Repo, mainBranch, &api.GetObjectParams{Path: migrateMultipartsFile})
	require.NoError(t, err, "failed to get object")
	require.Equal(t, http.StatusOK, getResp.StatusCode())

	preContentBytes, err := base64.StdEncoding.DecodeString(state.Multiparts.Content)
	require.NoError(t, err, "failed to decode error")

	fullContent := append(preContentBytes, partsConcat...)
	require.Equal(t, fullContent, getResp.Body)
}

// Actions Tests

func testPreMigrateActions(t *testing.T) {
	// Create action data before migration
	ctx, _, repo := setupTest(t)
	state.Actions.Repo = repo
	parseAndUploadActions(t, ctx, repo, mainBranch)
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())
	_, err = responseWithTimeout(server, 1*time.Minute) // pre-commit action triggered on action upload, flush buffer
	require.NoError(t, err)

	testCommitMerge(t, ctx, repo)
	testCreateDeleteBranch(t, ctx, repo)
	testCreateDeleteTag(t, ctx, repo)

	runResp := waitForListRepositoryRunsLen(ctx, t, repo, "", 13)
	for _, run := range runResp.Results {
		hookResp, err := client.ListRunHooksWithResponse(ctx, repo, run.RunId, &api.ListRunHooksParams{})
		require.NoError(t, err, "failed to list runs")
		require.Equal(t, http.StatusOK, hookResp.StatusCode())
		ar := new(actionRun)
		run.StartTime = run.StartTime.UTC()
		*run.EndTime = run.EndTime.UTC()
		ar.Run = run
		ar.Hooks = hookResp.JSON200.Results
		for i, task := range ar.Hooks {
			ar.Hooks[i].StartTime = task.StartTime.UTC()
			*ar.Hooks[i].EndTime = task.EndTime.UTC()
		}
		state.Actions.Runs = append(state.Actions.Runs, ar)
	}
	state.Actions.WebhookPort = server.port
}

func testPostMigrateActions(t *testing.T) {
	// Validate info using storage logs
	ctx, _, _ := setupTest(t)
	// Create new event and make sure it's listed before the migrated events
	resp, err := client.GetBranchWithResponse(ctx, state.Actions.Repo, mainBranch)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	commitID := resp.JSON200.CommitId

	createTagResp, err := client.CreateTagWithResponse(ctx, state.Actions.Repo, api.CreateTagJSONRequestBody{
		Id:  "failed-tag",
		Ref: commitID,
	})
	require.NoError(t, err, "failed to create tag")
	require.Equal(t, http.StatusInternalServerError, createTagResp.StatusCode())

	runs := waitForListRepositoryRunsLen(ctx, t, state.Actions.Repo, "", 14)
	runCount := 0
	for i, run := range runs.Results {
		if i < 1 { // First event is a failed pre create tag
			require.Equal(t, run.EventType, string(graveler.EventTypePreCreateTag))
			require.Equal(t, run.Status, "failed")
			continue
		}
		expRun := state.Actions.Runs[runCount].Run
		expTasks := state.Actions.Runs[runCount].Hooks
		// Ignore runID since it changes due to migration
		expRun.RunId = run.RunId
		require.Equal(t, expRun, run)
		runCount++
		hookResp, err := client.ListRunHooksWithResponse(ctx, state.Actions.Repo, run.RunId, &api.ListRunHooksParams{})
		require.NoError(t, err, "failed to list runs")
		require.Equal(t, http.StatusOK, hookResp.StatusCode())
		require.Equal(t, expTasks, hookResp.JSON200.Results)
	}

	// List by secondary index
	branch := mainBranch
	branchResp, err := client.ListRepositoryRunsWithResponse(ctx, state.Actions.Repo, &api.ListRepositoryRunsParams{Branch: &branch})
	require.NoError(t, err, "failed to list runs")
	require.Equal(t, len(branchResp.JSON200.Results), 3)
}

func testPreMigrateAuth(t *testing.T) {
	ctx, _, repo := setupTest(t)

	// creating a viewer, developer, superuser and admin and verifying their roles and permissions
	viewerCreds := createUserWithCredentialsInGroup(t, ctx, "testViewer", auth.ViewersGroup)
	developerCreds := createUserWithCredentialsInGroup(t, ctx, "testDeveloper", auth.DevelopersGroup)
	superUserCreds := createUserWithCredentialsInGroup(t, ctx, "testSuperUser", auth.SuperUsersGroup)
	adminCreds := createUserWithCredentialsInGroup(t, ctx, "testAdmin", auth.AdminsGroup)

	verifyUserPermissions(t, ctx, repo, "viewer", viewerCreds, viewerPermissions)
	verifyUserPermissions(t, ctx, repo, "developer", developerCreds, developerPermissions)
	verifyUserPermissions(t, ctx, repo, "superUser", superUserCreds, superUserPermissions)
	verifyUserPermissions(t, ctx, repo, "admin", adminCreds, adminPermissions)

	// creating a group with permissions, adding a user and verify permissions
	// the created group (and so, the user) has permissions to only list repositories, and so
	// is expected to succeed with that but to fail reading the repository. This is done to
	// later verify that a custom created groups and policies are migrated correctly
	respCreateGroup, err := client.CreateGroupWithResponse(ctx, api.CreateGroupJSONRequestBody{
		Id: authCustomGroupName,
	})
	require.NoError(t, err, "Admin failed while creating group")
	require.Equal(t, http.StatusCreated, respCreateGroup.StatusCode(), "Admin unexpectedly failed to create group")

	pid := "ListReposPolicy"
	respCreatePolicy, err := client.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
		CreationDate: api.Int64Ptr(time.Now().Unix()),
		Id:           pid,
		Statement: []api.Statement{
			{
				Action:   []string{"fs:ListRepositories"},
				Effect:   "allow",
				Resource: "*",
			},
		},
	})
	require.NoError(t, err, "Admin failed while creating policy")
	require.Equal(t, http.StatusCreated, respCreatePolicy.StatusCode(), "Admin unexpectedly failed to create policy")

	respAddPolicy, err := client.AttachPolicyToGroupWithResponse(ctx, authCustomGroupName, pid)
	require.NoError(t, err, "Admin failed while adding policy to group")
	require.Equal(t, http.StatusCreated, respAddPolicy.StatusCode(), "Admin unexpectedly failed to add policy to group")

	uid := "test-user"
	customCreds := createUserWithCredentialsInGroup(t, ctx, uid, authCustomGroupName)
	verifyUserPermissions(t, ctx, repo, "customUser", customCreds, customPermissions)

	// Hardening relevant test data for post-migrate
	state.Auth.Repo = repo
	state.Auth.ViewerUser.Credentials = &authCredentials{
		AccessKeyID:     viewerCreds.AccessKeyId,
		SecretAccessKey: viewerCreds.SecretAccessKey,
	}
	state.Auth.DeveloperUser.Credentials = &authCredentials{
		AccessKeyID:     developerCreds.AccessKeyId,
		SecretAccessKey: developerCreds.SecretAccessKey,
	}
	state.Auth.SuperUser.Credentials = &authCredentials{
		AccessKeyID:     superUserCreds.AccessKeyId,
		SecretAccessKey: superUserCreds.SecretAccessKey,
	}
	state.Auth.AdminUser.Credentials = &authCredentials{
		AccessKeyID:     adminCreds.AccessKeyId,
		SecretAccessKey: adminCreds.SecretAccessKey,
	}
	state.Auth.CustomUser.Credentials = &authCredentials{
		AccessKeyID:     customCreds.AccessKeyId,
		SecretAccessKey: customCreds.SecretAccessKey,
	}
}

func testPostMigrateAuth(t *testing.T) {
	ctx, _, _ := setupTest(t)

	// verifying all previous permissions are preserved through the migration process
	verifyUserPermissions(t, ctx, state.Auth.Repo, "viewer", &api.CredentialsWithSecret{
		AccessKeyId:     state.Auth.ViewerUser.Credentials.AccessKeyID,
		SecretAccessKey: state.Auth.ViewerUser.Credentials.SecretAccessKey,
	}, viewerPermissions)
	verifyUserPermissions(t, ctx, state.Auth.Repo, "developer", &api.CredentialsWithSecret{
		AccessKeyId:     state.Auth.DeveloperUser.Credentials.AccessKeyID,
		SecretAccessKey: state.Auth.DeveloperUser.Credentials.SecretAccessKey,
	}, developerPermissions)
	verifyUserPermissions(t, ctx, state.Auth.Repo, "superUser", &api.CredentialsWithSecret{
		AccessKeyId:     state.Auth.SuperUser.Credentials.AccessKeyID,
		SecretAccessKey: state.Auth.SuperUser.Credentials.SecretAccessKey,
	}, superUserPermissions)
	verifyUserPermissions(t, ctx, state.Auth.Repo, "admin", &api.CredentialsWithSecret{
		AccessKeyId:     state.Auth.AdminUser.Credentials.AccessKeyID,
		SecretAccessKey: state.Auth.AdminUser.Credentials.SecretAccessKey,
	}, adminPermissions)
	customUserCreds := &api.CredentialsWithSecret{
		AccessKeyId:     state.Auth.CustomUser.Credentials.AccessKeyID,
		SecretAccessKey: state.Auth.CustomUser.Credentials.SecretAccessKey,
	}

	// adding a policy to the custom created group (created on DB and migrated) and verify it is added successfully
	pid := "ReadReposPolicy"
	respCreatePolicy, err := client.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
		CreationDate: api.Int64Ptr(time.Now().Unix()),
		Id:           pid,
		Statement: []api.Statement{
			{
				Action:   []string{"fs:ReadRepository"},
				Effect:   "allow",
				Resource: "*",
			},
		},
	})
	require.NoError(t, err, "Admin failed while creating policy")
	require.Equal(t, http.StatusCreated, respCreatePolicy.StatusCode(), "Admin unexpectedly failed to create policy")

	respAddPolicy, err := client.AttachPolicyToGroupWithResponse(ctx, authCustomGroupName, pid)
	require.NoError(t, err, "Admin failed while adding policy to group")
	require.Equal(t, http.StatusCreated, respAddPolicy.StatusCode(), "Admin unexpectedly failed to add policy to group")
	customPermissions.canReadRepo = true
	verifyUserPermissions(t, ctx, state.Auth.Repo, "customUser", customUserCreds, customPermissions)
}

func createUserWithCredentialsInGroup(t *testing.T, ctx context.Context, username, groupID string) *api.CredentialsWithSecret {
	adminClient := client
	_, err := adminClient.CreateUserWithResponse(ctx, api.CreateUserJSONRequestBody{
		Id: username,
	})
	require.NoError(t, err, "Failed to create user "+username)

	_, err = adminClient.AddGroupMembershipWithResponse(ctx, groupID, username)
	require.NoError(t, err, "Failed to add group "+groupID+" to user "+username)

	// give the user access credentials
	r, err := adminClient.CreateCredentialsWithResponse(ctx, username)
	require.NoError(t, err, "Failed to create credentials for user "+username)
	require.Equal(t, http.StatusCreated, r.StatusCode(), "Failed to create credentials for user "+username)

	return r.JSON201
}

func verifyUserPermissions(t *testing.T, ctx context.Context, repo, userType string, creds *api.CredentialsWithSecret, userPerms *userPermissions) {
	endpointUrl := testutil.ParseEndpointURL(logger, viper.GetString("endpoint_url"))

	userClient, err := testutil.NewClientFromCreds(logger, creds.AccessKeyId, creds.SecretAccessKey, endpointUrl)
	require.NoError(t, err, "failed to initialize client for %s by credentials", userType)

	userID, _ := nanoid.New(6)
	userName := fmt.Sprintf("user-%s", userID)
	createUserResp, err := userClient.CreateUserWithResponse(ctx, api.CreateUserJSONRequestBody{
		Id: userName,
	})
	require.NoError(t, err, "%s failed to send CreaterUser request", userType)
	if userPerms.canCreateUser {
		require.Equal(t, http.StatusCreated, createUserResp.StatusCode(), "unexpected failure for %s - CreateUser", userType)
	} else {
		require.Equal(t, http.StatusUnauthorized, createUserResp.StatusCode(), "unexpected success for %s - CreateUser", userType)
	}

	listUsersResp, err := userClient.ListUsersWithResponse(ctx, &api.ListUsersParams{})
	require.NoError(t, err, "%s failed to send ListUsers request", userType)
	if userPerms.canListUsers {
		require.Equal(t, http.StatusOK, listUsersResp.StatusCode(), "unexpected failure for %s - ListUsers", userType)
	} else {
		require.Equal(t, http.StatusUnauthorized, listUsersResp.StatusCode(), "unexpected success for %s - ListUsers", userType)
	}

	listReposResp, err := userClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
	require.NoError(t, err, "failed to send ListRepositories request", userType)
	if userPerms.canListRepos {
		require.Equal(t, http.StatusOK, listReposResp.StatusCode(), "unexpected failure for %s - ListRepos", userType)
	} else {
		require.Equal(t, http.StatusUnauthorized, listReposResp.StatusCode(), "unexpected success for %s - ListRepos", userType)
	}

	getRepoResp, err := userClient.GetRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "failed to send GetRepository request", userType)
	if userPerms.canReadRepo {
		require.Equal(t, http.StatusOK, getRepoResp.StatusCode(), "unexpected failure for %s - GetRepo", userType)
	} else {
		require.Equal(t, http.StatusUnauthorized, getRepoResp.StatusCode(), "unexpected success for %s - GetRepo", userType)
	}

	branchID, _ := nanoid.New(6)
	branchName := fmt.Sprintf("br-%s", branchID)
	respCreateBranch, err := userClient.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branchName,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to send CreateBranch request", userType)
	if userPerms.canCreateBranch {
		require.Equal(t, http.StatusCreated, respCreateBranch.StatusCode(), "unexpected failure for %s - CreateBranch", userType)
	} else {
		require.Equal(t, http.StatusUnauthorized, respCreateBranch.StatusCode(), "unexpected success for %s - CreateBranch", userType)
	}

	respDeleteBranch, err := userClient.DeleteBranchWithResponse(ctx, repo, branchName)
	require.NoError(t, err, "failed to send DeleteBranch request", userType)
	if userPerms.canCreateBranch {
		require.Equal(t, http.StatusNoContent, respDeleteBranch.StatusCode(), "unexpected failure for %s - DeleteBranch", userType)
	} else {
		require.Equal(t, http.StatusUnauthorized, respDeleteBranch.StatusCode(), "unexpected success for %s - DeleteBranch", userType)
	}
}
