package esti

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

type hooksValidationData struct {
	data []*webhookEventInfo
	mu   sync.RWMutex
}

func (h *hooksValidationData) appendRes(info webhookEventInfo) {
	h.mu.Lock()
	h.data = append(h.data, &info)
	h.mu.Unlock()
}

func HooksSuccessTest(ctx context.Context, t *testing.T, repo string) {
	var htd = hooksValidationData{
		data: make([]*webhookEventInfo, 0),
		mu:   sync.RWMutex{},
	}
	parseAndUploadActions(t, ctx, repo, mainBranch)
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	webhookData, err := ResponseWithTimeout(server, 1*time.Minute) // pre-commit action triggered on action upload, flush buffer
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on pre commit serving")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preCommitEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCommitEvent), "reading pre-commit data")
	htd.appendRes(preCommitEvent)

	t.Run("commit merge test", func(t *testing.T) {
		testCommitMerge(t, ctx, repo, &htd)
	})
	t.Run("create delete branch test", func(t *testing.T) {
		testCreateDeleteBranch(t, ctx, repo, &htd)
	})
	t.Run("create delete tag test", func(t *testing.T) {
		testCreateDeleteTag(t, ctx, repo, &htd)
	})

	t.Log("check runs are sorted in descending order")
	runs := WaitForListRepositoryRunsLen(ctx, t, repo, "", 13, nil)
	require.Equal(t, len(runs.Results), len(htd.data))
	for i, run := range runs.Results {
		valIdx := len(htd.data) - (i + 1)
		require.Equal(t, htd.data[valIdx].EventType, run.EventType)
	}
}

func testCommitMerge(t *testing.T, ctx context.Context, repo string, htd *hooksValidationData) {
	const branch = "feature-1"

	t.Log("Create branch", branch)
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
	t.Log("Branch created", ref)

	resp, err := UploadContent(ctx, repo, branch, "somefile", "", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	t.Log("Commit content", branch)
	commitResp, err := client.CommitWithResponse(ctx, repo, branch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	webhookData, err := ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on pre commit serving")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preCommitEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCommitEvent), "reading pre-commit data")
	htd.appendRes(preCommitEvent)
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
	require.NotNil(t, webhookData.QueryParams)
	require.Contains(t, webhookData.QueryParams, "check_env_vars")
	require.Equal(t, []string{"this_is_actions_var"}, webhookData.QueryParams["check_env_vars"])

	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on post commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var postCommitEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postCommitEvent), "reading post-commit data")
	htd.appendRes(postCommitEvent)
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

	mergeResp, err := client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, apigen.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err)

	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, mergeResp.StatusCode())
	require.NoError(t, webhookData.Err, "failed to merge branches")
	mergeRef := mergeResp.JSON200.Reference
	t.Log("Merged successfully", mergeRef)

	require.NoError(t, err, "error on pre commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preMergeEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preMergeEvent), "reading pre-merge data")
	htd.appendRes(preMergeEvent)
	require.Equal(t, webhookEventInfo{
		EventTime:     preMergeEvent.EventTime,
		SourceRef:     commitRecord.Id,
		EventType:     "pre-merge",
		ActionName:    "Test Pre Merge",
		HookID:        "test_webhook",
		RepositoryID:  repo,
		MergeSource:   branch,
		BranchID:      mainBranch,
		Committer:     commitRecord.Committer,
		CommitMessage: fmt.Sprintf("Merge '%s' into '%s'", branch, mainBranch),
		CommitID:      mergeRef,
	}, preMergeEvent)

	// Testing post-merge hook response
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var postMergeEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postMergeEvent), "reading post-merge data")
	htd.appendRes(postMergeEvent)
	require.Equal(t, webhookEventInfo{
		EventTime:     postMergeEvent.EventTime,
		SourceRef:     mergeResp.JSON200.Reference,
		EventType:     "post-merge",
		ActionName:    "Test Post Merge",
		HookID:        "test_webhook",
		RepositoryID:  repo,
		MergeSource:   branch,
		BranchID:      mainBranch,
		CommitID:      mergeRef,
		Committer:     commitRecord.Committer,
		CommitMessage: fmt.Sprintf("Merge '%s' into '%s'", branch, mainBranch),
	}, postMergeEvent)

	t.Log("List repository runs", mergeRef)
	runs := WaitForListRepositoryRunsLen(ctx, t, repo, mergeRef, 2, nil)
	eventType := map[string]bool{
		"pre-merge":  true,
		"post-merge": true,
	}
	for _, run := range runs.Results {
		require.Equal(t, mergeRef, run.CommitId)
		require.True(t, eventType[run.EventType])
		eventType[run.EventType] = false
		require.Equal(t, "completed", run.Status)
		require.Equal(t, "main", run.Branch)
	}
}

func testCreateDeleteBranch(t *testing.T, ctx context.Context, repo string, htd *hooksValidationData) {
	const testBranch = "test_branch_delete"
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
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
	webhookData, err := ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on pre create branch")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preCreateBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCreateBranchEvent), "reading pre-create branch data")
	htd.appendRes(preCreateBranchEvent)
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
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on post create branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var postCreateBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postCreateBranchEvent), "reading post-create branch data")
	htd.appendRes(postCreateBranchEvent)
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
	deleteBranchResp, err := client.DeleteBranchWithResponse(ctx, repo, testBranch, &apigen.DeleteBranchParams{})

	require.NoError(t, err, "failed to delete branch")
	require.Equal(t, http.StatusNoContent, deleteBranchResp.StatusCode())

	// Testing pre-delete branch hook response
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on pre delete branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preDeleteBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preDeleteBranchEvent), "reading pre-delete branch data")
	htd.appendRes(preDeleteBranchEvent)
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
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on post delete branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var postDeleteBranchEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postDeleteBranchEvent), "reading post-delete branch data")
	htd.appendRes(postDeleteBranchEvent)
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

func testCreateDeleteTag(t *testing.T, ctx context.Context, repo string, htd *hooksValidationData) {
	const tagID = "tag_test_hooks"

	resp, err := client.GetBranchWithResponse(ctx, repo, mainBranch)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	commitID := resp.JSON200.CommitId

	createTagResp, err := client.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
		Id:  tagID,
		Ref: commitID,
	})

	require.NoError(t, err, "failed to create tag")
	require.Equal(t, http.StatusCreated, createTagResp.StatusCode())

	// Testing pre-create tag hook response
	webhookData, err := ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on pre create tag")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preCreateTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preCreateTagEvent), "reading pre-create tag data")
	htd.appendRes(preCreateTagEvent)
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
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on post create tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var postCreateTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postCreateTagEvent), "reading post-create tag data")
	htd.appendRes(postCreateTagEvent)
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
	deleteTagResp, err := client.DeleteTagWithResponse(ctx, repo, tagID, &apigen.DeleteTagParams{})

	require.NoError(t, err, "failed to delete tag")
	require.Equal(t, http.StatusNoContent, deleteTagResp.StatusCode())

	// Testing pre-delete tag hook response
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on pre delete tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var preDeleteTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&preDeleteTagEvent), "reading pre-delete tag data")
	htd.appendRes(preDeleteTagEvent)
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
	webhookData, err = ResponseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.Err, "error on post delete tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.Data))
	var postDeleteTagEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&postDeleteTagEvent), "reading post-delete tag data")
	htd.appendRes(postDeleteTagEvent)
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

	actionsDir, _ := fs.Sub(ActionsPath, "action_files")
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
		resp, err := UploadContent(ctx, repo, branch, "_lakefs_actions/"+ent, action, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
	}

	// wait 8 seconds to let the actions cache expire.
	time.Sleep(8 * time.Second)
}

type webhookEventInfo struct {
	EventType     string            `json:"event_type"`
	EventTime     string            `json:"event_time"`
	ActionName    string            `json:"action_name"`
	HookID        string            `json:"hook_id"`
	RepositoryID  string            `json:"repository_id"`
	BranchID      string            `json:"branch_id"`
	MergeSource   string            `json:"merge_source"`
	SourceRef     string            `json:"source_ref"`
	TagID         string            `json:"tag_id"`
	CommitID      string            `json:"commit_id"`
	CommitMessage string            `json:"commit_message"`
	Committer     string            `json:"committer"`
	Metadata      map[string]string `json:"metadata"`
}
