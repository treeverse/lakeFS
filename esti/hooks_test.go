package esti

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

//go:embed action_files/*.yaml
var actions embed.FS

func TestHooksSuccess(t *testing.T) {
	ctx, _, repo := setupTest(t)
	parseAndUploadActions(t, ctx, repo, mainBranch)
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	_, err = responseWithTimeout(server, 1*time.Minute) // pre-commit action triggered on action upload, flush buffer
	require.NoError(t, err)

	t.Run("commit merge test", func(t *testing.T) {
		testCommitMerge(t, ctx, repo)
	})
	t.Run("create delete branch test", func(t *testing.T) {
		testCreateDeleteBranch(t, ctx, repo)
	})
	t.Run("create delete tag test", func(t *testing.T) {
		testCreateDeleteTag(t, ctx, repo)
	})
}

func testCommitMerge(t *testing.T, ctx context.Context, repo string) {
	const branch = "feature-1"

	t.Log("Create branch", branch)
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
	t.Log("Branch created", ref)

	resp, err := uploadContent(ctx, repo, branch, "somefile", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	t.Log("Commit content", branch)
	commitResp, err := client.CommitWithResponse(ctx, repo, branch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	webhookData, err := responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre commit serving")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.data))
	var preCommitEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCommitEvent), "reading pre-commit data")
	commitRecord := commitResp.JSON201
	require.Equal(t, webhookEventInfo{
		EventTime:     preCommitEvent.EventTime,
		SourceRef:     branch,
		EventType:     "pre-commit",
		ActionName:    "Test Pre Commit",
		HookID:        "test_webhook",
		RepositoryID:  repo,
		BranchID:      branch,
		Committer:     commitRecord.Committer,
		CommitMessage: commitRecord.Message,
		Metadata:      commitRecord.Metadata.AdditionalProperties,
	}, preCommitEvent)
	require.NotNil(t, webhookData.queryParams)
	require.Contains(t, webhookData.queryParams, "check_env_vars")
	require.Equal(t, []string{"this_is_actions_var"}, webhookData.queryParams["check_env_vars"])

	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var postCommitEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postCommitEvent), "reading post-commit data")
	require.Equal(t, webhookEventInfo{
		EventTime:     postCommitEvent.EventTime,
		SourceRef:     commitResp.JSON201.Id,
		EventType:     "post-commit",
		ActionName:    "Test Post Commit",
		HookID:        "test_webhook",
		RepositoryID:  repo,
		BranchID:      branch,
		CommitID:      commitRecord.Id,
		Committer:     commitRecord.Committer,
		CommitMessage: commitRecord.Message,
		Metadata:      commitRecord.Metadata.AdditionalProperties,
	}, postCommitEvent)

	mergeResp, err := client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, api.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err)

	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, mergeResp.StatusCode())
	require.NoError(t, webhookData.err, "failed to merge branches")
	mergeRef := mergeResp.JSON200.Reference
	t.Log("Merged successfully", mergeRef)

	require.NoError(t, err, "error on pre commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var preMergeEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preMergeEvent), "reading pre-merge data")
	require.Equal(t, webhookEventInfo{
		EventTime:     preMergeEvent.EventTime,
		SourceRef:     commitRecord.Id,
		EventType:     "pre-merge",
		ActionName:    "Test Pre Merge",
		HookID:        "test_webhook",
		RepositoryID:  repo,
		BranchID:      mainBranch,
		Committer:     commitRecord.Committer,
		CommitMessage: fmt.Sprintf("Merge '%s' into '%s'", branch, mainBranch),
	}, preMergeEvent)

	// Testing post-merge hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var postMergeEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postMergeEvent), "reading post-merge data")
	require.Equal(t, webhookEventInfo{
		EventTime:     postMergeEvent.EventTime,
		SourceRef:     mergeResp.JSON200.Reference,
		EventType:     "post-merge",
		ActionName:    "Test Post Merge",
		HookID:        "test_webhook",
		RepositoryID:  repo,
		BranchID:      mainBranch,
		CommitID:      mergeResp.JSON200.Reference,
		Committer:     commitRecord.Committer,
		CommitMessage: fmt.Sprintf("Merge '%s' into '%s'", branch, mainBranch),
	}, postMergeEvent)

	t.Log("List repository runs", mergeRef)
	runsResp, err := client.ListRepositoryRunsWithResponse(ctx, repo, &api.ListRepositoryRunsParams{
		Commit: api.StringPtr(mergeRef),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, runsResp.StatusCode())
	runs := runsResp.JSON200
	require.Len(t, runs.Results, 1)
	run := runs.Results[0]
	require.Equal(t, mergeRef, run.CommitId)
	require.Equal(t, "pre-merge", run.EventType)
	require.Equal(t, "completed", run.Status)
	require.Equal(t, "main", run.Branch)
}

func testCreateDeleteBranch(t *testing.T, ctx context.Context, repo string) {
	const testBranch = "test_branch_delete"
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   testBranch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())

	resp, err := client.GetBranchWithResponse(ctx, repo, mainBranch)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	commitID := resp.JSON200.CommitId

	// Testing pre-create branch hook response
	webhookData, err := responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre create branch")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.data))
	var preCreateBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCreateBranchEvent), "reading pre-create branch data")
	require.Equal(t, webhookEventInfo{
		EventTime:    preCreateBranchEvent.EventTime,
		SourceRef:    mainBranch,
		EventType:    "pre-create-branch",
		ActionName:   "Test Pre Create Branch",
		HookID:       "test_webhook",
		RepositoryID: repo,
		BranchID:     testBranch,
		CommitID:     commitID,
	}, preCreateBranchEvent)

	// Testing post-create branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post create branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var postCreateBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postCreateBranchEvent), "reading post-create branch data")
	require.Equal(t, webhookEventInfo{
		EventTime:    postCreateBranchEvent.EventTime,
		SourceRef:    mainBranch,
		EventType:    "post-create-branch",
		ActionName:   "Test Post Create Branch",
		HookID:       "test_webhook",
		RepositoryID: repo,
		BranchID:     testBranch,
		CommitID:     commitID,
	}, postCreateBranchEvent)

	// Delete branch
	deleteBranchResp, err := client.DeleteBranchWithResponse(ctx, repo, testBranch)

	require.NoError(t, err, "failed to delete branch")
	require.Equal(t, http.StatusNoContent, deleteBranchResp.StatusCode())

	// Testing pre-delete branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre delete branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var preDeleteBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preDeleteBranchEvent), "reading pre-delete branch data")
	require.Equal(t, webhookEventInfo{
		EventTime:    preDeleteBranchEvent.EventTime,
		SourceRef:    commitID,
		EventType:    "pre-delete-branch",
		ActionName:   "Test Pre Delete Branch",
		HookID:       "test_webhook",
		RepositoryID: repo,
		BranchID:     testBranch,
	}, preDeleteBranchEvent)

	// Testing post-delete branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post delete branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var postDeleteBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postDeleteBranchEvent), "reading post-delete branch data")
	require.Equal(t, webhookEventInfo{
		EventTime:    postDeleteBranchEvent.EventTime,
		SourceRef:    commitID,
		EventType:    "post-delete-branch",
		ActionName:   "Test Post Delete Branch",
		HookID:       "test_webhook",
		RepositoryID: repo,
		BranchID:     testBranch,
	}, postDeleteBranchEvent)
}

func testCreateDeleteTag(t *testing.T, ctx context.Context, repo string) {
	const tagID = "tag_test_hooks"

	resp, err := client.GetBranchWithResponse(ctx, repo, mainBranch)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	commitID := resp.JSON200.CommitId

	createTagResp, err := client.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
		Id:  tagID,
		Ref: commitID,
	})

	require.NoError(t, err, "failed to create tag")
	require.Equal(t, http.StatusCreated, createTagResp.StatusCode())

	// Testing pre-create tag hook response
	webhookData, err := responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre create tag")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.data))
	var preCreateTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCreateTagEvent), "reading pre-create tag data")
	require.Equal(t, webhookEventInfo{
		EventTime:    preCreateTagEvent.EventTime,
		SourceRef:    commitID,
		EventType:    "pre-create-tag",
		ActionName:   "Test Pre Create Tag",
		HookID:       "test_webhook",
		RepositoryID: repo,
		CommitID:     commitID,
		TagID:        tagID,
	}, preCreateTagEvent)

	// Testing post-create tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post create tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var postCreateTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postCreateTagEvent), "reading post-create tag data")
	require.Equal(t, webhookEventInfo{
		EventTime:    postCreateTagEvent.EventTime,
		SourceRef:    commitID,
		EventType:    "post-create-tag",
		ActionName:   "Test Post Create Tag",
		HookID:       "test_webhook",
		RepositoryID: repo,
		CommitID:     commitID,
		TagID:        tagID,
	}, postCreateTagEvent)

	// Delete tag
	deleteTagResp, err := client.DeleteTagWithResponse(ctx, repo, tagID)

	require.NoError(t, err, "failed to delete tag")
	require.Equal(t, http.StatusNoContent, deleteTagResp.StatusCode())

	// Testing pre-delete tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre delete tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var preDeleteTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preDeleteTagEvent), "reading pre-delete tag data")
	require.Equal(t, webhookEventInfo{
		EventTime:    preDeleteTagEvent.EventTime,
		SourceRef:    commitID,
		EventType:    "pre-delete-tag",
		ActionName:   "Test Pre Delete Tag",
		HookID:       "test_webhook",
		RepositoryID: repo,
		CommitID:     commitID,
		TagID:        tagID,
	}, preDeleteTagEvent)

	// Testing post-delete tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post delete tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	var postDeleteTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postDeleteTagEvent), "reading post-delete tag data")
	require.Equal(t, webhookEventInfo{
		EventTime:    postDeleteTagEvent.EventTime,
		SourceRef:    commitID,
		EventType:    "post-delete-tag",
		ActionName:   "Test Post Delete Tag",
		HookID:       "test_webhook",
		RepositoryID: repo,
		CommitID:     commitID,
		TagID:        tagID,
	}, postDeleteTagEvent)
}

func parseAndUploadActions(t *testing.T, ctx context.Context, repo, branch string) {
	t.Helper()
	// render actions based on templates
	docData := struct {
		URL string
	}{
		URL: server.BaseURL(),
	}

	actionsDir, _ := fs.Sub(actions, "action_files")
	ents, _ := fs.Glob(actionsDir, "*.yaml")
	for _, ent := range ents {
		buf, err := fs.ReadFile(actionsDir, ent)
		require.NoError(t, err)

		actionTmpl, err := template.New(ent).Parse(string(buf))
		require.NoError(t, err, ent)
		var doc bytes.Buffer
		err = actionTmpl.Execute(&doc, docData)
		require.NoError(t, err)

		action := doc.String()
		resp, err := uploadContent(ctx, repo, branch, "_lakefs_actions/"+ent, action)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
	}
}

type webhookEventInfo struct {
	EventType     string            `json:"event_type"`
	EventTime     string            `json:"event_time"`
	ActionName    string            `json:"action_name"`
	HookID        string            `json:"hook_id"`
	RepositoryID  string            `json:"repository_id"`
	BranchID      string            `json:"branch_id"`
	SourceRef     string            `json:"source_ref"`
	TagID         string            `json:"tag_id"`
	CommitID      string            `json:"commit_id"`
	CommitMessage string            `json:"commit_message"`
	Committer     string            `json:"committer"`
	Metadata      map[string]string `json:"metadata"`
}
