package nessie

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"strings"
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
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	_, err = responseWithTimeout(server, 1*time.Minute) // pre-commit action triggered on action upload, flush buffer
	require.NoError(t, err)

	tests := []struct {
		name     string
		testFunc func(*testing.T, context.Context, string)
		err      error
	}{
		{
			name:     "commit merge test",
			testFunc: commitMergeTest,
		},
		{
			name:     "create delete branch test",
			testFunc: CreateDeleteBranchTest,
		},
		{
			name:     "create delete tag test",
			testFunc: CreateDeleteTagTest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t, ctx, repo)
		})
	}
}

func commitMergeTest(t *testing.T, ctx context.Context, repo string) {
	const branch = "feature-1"
	var preCommitEvent, postCommitEvent, preMergeEvent, postMergeEvent webhookEventInfo

	logger.WithField("branch", branch).Info("Create branch")
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
	logger.WithField("branchRef", ref).Info("Branch created")

	resp, err := uploadContent(ctx, repo, branch, "somefile", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	logger.WithField("branch", branch).Info("Commit content")
	commitResp, err := client.CommitWithResponse(ctx, repo, branch, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	webhookData, err := responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre commit serving")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&preCommitEvent), "reading pre-commit data")

	commitRecord := commitResp.JSON201
	require.Equal(t, "pre-commit", preCommitEvent.EventType)
	require.Equal(t, "Test Pre Commit", preCommitEvent.ActionName)
	require.Equal(t, "test_webhook", preCommitEvent.HookID)
	require.Equal(t, repo, preCommitEvent.RepositoryID)
	require.Equal(t, branch, preCommitEvent.BranchID)
	require.Equal(t, commitRecord.Committer, preCommitEvent.Committer)
	require.Equal(t, commitRecord.Message, preCommitEvent.CommitMessage)
	require.Equal(t, branch, preCommitEvent.SourceRef)
	require.Equal(t, commitRecord.Metadata.AdditionalProperties, preCommitEvent.Metadata)
	require.NotNil(t, webhookData.queryParams)
	require.Contains(t, webhookData.queryParams, "check_env_vars")
	require.Equal(t, []string{"this_is_actions_var"}, webhookData.queryParams["check_env_vars"])

	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&postCommitEvent), "reading post-commit data")

	require.Equal(t, "post-commit", postCommitEvent.EventType)
	require.Equal(t, "Test Post Commit", postCommitEvent.ActionName)
	require.Equal(t, "test_webhook", postCommitEvent.HookID)
	require.Equal(t, repo, postCommitEvent.RepositoryID)
	require.Equal(t, branch, postCommitEvent.BranchID)
	require.Equal(t, commitRecord.Committer, postCommitEvent.Committer)
	require.Equal(t, commitRecord.Message, postCommitEvent.CommitMessage)
	require.Equal(t, commitResp.JSON201.Id, postCommitEvent.SourceRef)
	require.Equal(t, commitRecord.Metadata.AdditionalProperties, postCommitEvent.Metadata)

	mergeResp, err := client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, api.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err)

	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, mergeResp.StatusCode())
	require.NoError(t, webhookData.err, "failed to merge branches")
	mergeRef := mergeResp.JSON200.Reference
	logger.WithField("mergeResult", mergeRef).Info("Merged successfully")

	require.NoError(t, err, "error on pre commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&preMergeEvent), "reading pre-merge data")

	require.Equal(t, "pre-merge", preMergeEvent.EventType)
	require.Equal(t, "Test Pre Merge", preMergeEvent.ActionName)
	require.Equal(t, "test_webhook", preMergeEvent.HookID)
	require.Equal(t, repo, preMergeEvent.RepositoryID)
	require.Equal(t, mainBranch, preMergeEvent.BranchID)
	require.Equal(t, commitRecord.Id, preMergeEvent.SourceRef)

	// Testing post-merge hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&postMergeEvent), "reading post-merge data")

	require.Equal(t, "post-merge", postMergeEvent.EventType)
	require.Equal(t, "Test Post Merge", postMergeEvent.ActionName)
	require.Equal(t, "test_webhook", postMergeEvent.HookID)
	require.Equal(t, repo, postMergeEvent.RepositoryID)
	require.Equal(t, mainBranch, postMergeEvent.BranchID)
	require.Equal(t, mergeResp.JSON200.Reference, postMergeEvent.SourceRef)

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

func CreateDeleteBranchTest(t *testing.T, ctx context.Context, repo string) {
	var preCreateBranchEvent, postCreateBranchEvent, preDeleteBranchEvent, postDeleteBranchEvent webhookEventInfo
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
	require.NoError(t, decoder.Decode(&preCreateBranchEvent), "reading pre-create branch data")
	require.Equal(t, "pre-create-branch", preCreateBranchEvent.EventType)
	require.Equal(t, "Test Pre Create Branch", preCreateBranchEvent.ActionName)
	require.Equal(t, "test_webhook", preCreateBranchEvent.HookID)
	require.Equal(t, repo, preCreateBranchEvent.RepositoryID)
	require.Equal(t, mainBranch, preCreateBranchEvent.SourceRef)
	require.Equal(t, testBranch, preCreateBranchEvent.BranchID)
	require.Equal(t, commitID, preCreateBranchEvent.CommitID)

	// Testing post-create branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post create branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&postCreateBranchEvent), "reading post-create branch data")
	require.Equal(t, "post-create-branch", postCreateBranchEvent.EventType)
	require.Equal(t, "Test Post Create Branch", postCreateBranchEvent.ActionName)
	require.Equal(t, "test_webhook", postCreateBranchEvent.HookID)
	require.Equal(t, repo, postCreateBranchEvent.RepositoryID)
	require.Equal(t, mainBranch, postCreateBranchEvent.SourceRef)
	require.Equal(t, testBranch, postCreateBranchEvent.BranchID)
	require.Equal(t, commitID, postCreateBranchEvent.CommitID)

	// Delete branch
	deleteBranchResp, err := client.DeleteBranchWithResponse(ctx, repo, testBranch)

	require.NoError(t, err, "failed to delete branch")
	require.Equal(t, http.StatusNoContent, deleteBranchResp.StatusCode())

	// Testing pre-delete branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre delete branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&preDeleteBranchEvent), "reading pre-delete branch data")
	require.Equal(t, "pre-delete-branch", preDeleteBranchEvent.EventType)
	require.Equal(t, "Test Pre Delete Branch", preDeleteBranchEvent.ActionName)
	require.Equal(t, "test_webhook", preDeleteBranchEvent.HookID)
	require.Equal(t, repo, preDeleteBranchEvent.RepositoryID)
	require.Equal(t, testBranch, postCreateBranchEvent.BranchID)

	// Testing post-delete branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post delete branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&postDeleteBranchEvent), "reading post-delete branch data")
	require.Equal(t, "post-delete-branch", postDeleteBranchEvent.EventType)
	require.Equal(t, "Test Post Delete Branch", postDeleteBranchEvent.ActionName)
	require.Equal(t, "test_webhook", postDeleteBranchEvent.HookID)
	require.Equal(t, repo, postDeleteBranchEvent.RepositoryID)
	require.Equal(t, testBranch, postCreateBranchEvent.BranchID)
}

func CreateDeleteTagTest(t *testing.T, ctx context.Context, repo string) {
	const tagID = "tag_test_hooks"
	var preCreateTagEvent, postCreateTagEvent, preDeleteTagEvent, postDeleteTagEvent webhookEventInfo

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
	require.NoError(t, decoder.Decode(&preCreateTagEvent), "reading pre-create tag data")
	require.Equal(t, "pre-create-tag", preCreateTagEvent.EventType)
	require.Equal(t, "Test Pre Create Tag", preCreateTagEvent.ActionName)
	require.Equal(t, "test_webhook", preCreateTagEvent.HookID)
	require.Equal(t, repo, preCreateTagEvent.RepositoryID)
	require.Equal(t, commitID, preCreateTagEvent.CommitID)
	require.Equal(t, tagID, preCreateTagEvent.TagID)

	// Testing post-create tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post create tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&postCreateTagEvent), "reading post-create tag data")
	require.Equal(t, "post-create-tag", postCreateTagEvent.EventType)
	require.Equal(t, "Test Post Create Tag", postCreateTagEvent.ActionName)
	require.Equal(t, "test_webhook", postCreateTagEvent.HookID)
	require.Equal(t, repo, postCreateTagEvent.RepositoryID)
	require.Equal(t, commitID, postCreateTagEvent.CommitID)
	require.Equal(t, tagID, postCreateTagEvent.TagID)

	// Delete tag
	deleteTagResp, err := client.DeleteTagWithResponse(ctx, repo, tagID)

	require.NoError(t, err, "failed to delete tag")
	require.Equal(t, http.StatusNoContent, deleteTagResp.StatusCode())

	// Testing pre-delete tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre delete tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&preDeleteTagEvent), "reading pre-delete tag data")
	require.Equal(t, "pre-delete-tag", preDeleteTagEvent.EventType)
	require.Equal(t, "Test Pre Delete Tag", preDeleteTagEvent.ActionName)
	require.Equal(t, "test_webhook", preDeleteTagEvent.HookID)
	require.Equal(t, repo, preDeleteTagEvent.RepositoryID)
	require.Equal(t, commitID, preDeleteTagEvent.SourceRef)
	require.Equal(t, tagID, preDeleteTagEvent.TagID)

	// Testing post-delete tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on post delete tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&postDeleteTagEvent), "reading post-delete tag data")
	require.Equal(t, "post-delete-tag", postDeleteTagEvent.EventType)
	require.Equal(t, "Test Post Delete Tag", postDeleteTagEvent.ActionName)
	require.Equal(t, "test_webhook", postDeleteTagEvent.HookID)
	require.Equal(t, repo, postDeleteTagEvent.RepositoryID)
	require.Equal(t, commitID, postDeleteTagEvent.SourceRef)
	require.Equal(t, tagID, postDeleteTagEvent.TagID)
}

func parseAndUploadActions(t *testing.T, ctx context.Context, repo, branch string) {
	t.Helper()
	// render actions based on templates
	docData := struct {
		URL string
	}{
		URL: server.BaseURL(),
	}

	fs.WalkDir(actions, ".", func(path string, d fs.DirEntry, err error) error {
		var doc bytes.Buffer
		actionPath := strings.ReplaceAll(d.Name(), "_", "-")

		require.NoError(t, err)
		if !d.IsDir() {
			ba, err := actions.ReadFile(path)
			require.NoError(t, err)

			actionTmpl := template.Must(template.New(d.Name()).Parse(string(ba)))
			err = actionTmpl.Execute(&doc, docData)
			require.NoError(t, err)

			action := doc.String()
			resp, err := uploadContent(ctx, repo, branch, "_lakefs_actions/"+actionPath, action)
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, resp.StatusCode())
		}
		return nil
	})
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
