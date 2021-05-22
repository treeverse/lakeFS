package nessie

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

const actionPreMergeYaml = `
name: Test Merge
description: set of checks to verify that branch is good
on:
  pre-merge:
    branches:
      - main
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for pre-merge works
    properties:
      url: "{{.URL}}/pre-merge"
`

const actionPreCommitYaml = `
name: Test Commit
description: set of checks to verify that branch is good
on:
  pre-commit:
    branches:
      - feature-*
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for pre-commit works
    properties:
      url: "{{.URL}}/pre-commit"
`

func TestHooksSuccess(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	const branch = "feature-1"

	logger.WithField("branch", branch).Info("Create branch")
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
	logger.WithField("branchRef", ref).Info("Branch created")
	logger.WithField("branch", branch).Info("Upload initial content")

	// render actions based on templates
	docData := struct {
		URL string
	}{
		URL: server.BaseURL(),
	}

	actionPreMergeTmpl := template.Must(template.New("action-pre-merge").Parse(actionPreMergeYaml))
	var doc bytes.Buffer
	err = actionPreMergeTmpl.Execute(&doc, docData)
	require.NoError(t, err)
	preMergeAction := doc.String()

	resp, err := uploadContent(ctx, repo, branch, "_lakefs_actions/testing_pre_merge", preMergeAction)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	actionPreCommitTmpl := template.Must(template.New("action-pre-commit").Parse(actionPreCommitYaml))
	doc.Reset()
	err = actionPreCommitTmpl.Execute(&doc, docData)
	require.NoError(t, err)
	preCommitAction := doc.String()

	uploadResp, err := uploadContent(ctx, repo, branch, "_lakefs_actions/testing_pre_commit", preCommitAction)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode())
	logger.WithField("branch", branch).Info("Commit initial content")

	commitResp, err := client.CommitWithResponse(ctx, repo, branch, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	webhookData, err := responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre commit serving")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.data))
	var commitEvent, mergeEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&commitEvent), "reading pre-commit data")

	commitRecord := commitResp.JSON201
	require.Equal(t, "pre-commit", commitEvent.EventType)
	require.Equal(t, "Test Commit", commitEvent.ActionName)
	require.Equal(t, "test_webhook", commitEvent.HookID)
	require.Equal(t, repo, commitEvent.RepositoryID)
	require.Equal(t, branch, commitEvent.BranchID)
	require.Equal(t, commitRecord.Committer, commitEvent.Committer)
	require.Equal(t, commitRecord.Message, commitEvent.CommitMessage)
	require.Equal(t, branch, commitEvent.SourceRef)
	require.Equal(t, commitRecord.Metadata.AdditionalProperties, commitEvent.Metadata)

	mergeResp, err := client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, api.MergeIntoBranchJSONRequestBody{})

	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, mergeResp.StatusCode())
	require.NoError(t, webhookData.err, "failed to merge branches")
	mergeRef := mergeResp.JSON200.Reference
	logger.WithField("mergeResult", mergeRef).Info("Merged successfully")

	require.NoError(t, err, "error on pre commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&mergeEvent), "reading pre-merge data")

	require.Equal(t, "pre-merge", mergeEvent.EventType)
	require.Equal(t, "Test Merge", mergeEvent.ActionName)
	require.Equal(t, "test_webhook", mergeEvent.HookID)
	require.Equal(t, repo, mergeEvent.RepositoryID)
	require.Equal(t, mainBranch, mergeEvent.BranchID)
	require.Equal(t, commitRecord.Id, mergeEvent.SourceRef)

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

type webhookEventInfo struct {
	EventType     string            `json:"event_type"`
	EventTime     string            `json:"event_time"`
	ActionName    string            `json:"action_name"`
	HookID        string            `json:"hook_id"`
	RepositoryID  string            `json:"repository_id"`
	BranchID      string            `json:"branch_id"`
	SourceRef     string            `json:"source_ref"`
	CommitMessage string            `json:"commit_message"`
	Committer     string            `json:"committer"`
	Metadata      map[string]string `json:"metadata"`
}
