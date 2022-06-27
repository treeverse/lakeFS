package esti

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

// state to be used
var state migrateTestState

type migrateTestState struct {
	Multiparts multipartsState
	Actions    actionsState
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

const (
	migrateMultipartsFile     = "multipart_file"
	migrateMultipartsFilepath = mainBranch + "/" + migrateMultipartsFile
	migrateStateRepoName      = "migrate"
	migrateStateObjectPath    = "state.json"
	migratePrePartsCount      = 3
	migratePostPartsCount     = 2
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

	saveStateInLakeFS(t)
}

func postMigrateTests(t *testing.T) {
	readStateFromLakeFS(t)

	// all post tests execution
	t.Run("TestPostMigrateMultipart", testPostMigrateMultipart)
	t.Run("testPostMigrateActions", testPostMigrateActions)
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
