package nessie

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

const actionPreMergeYaml = `
name: Test Pre Merge
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
name: Test Pre Commit
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
      query_params:
        check_env_vars: "{{"{{ ENV.ACTIONS_VAR }}"}}"
`

const actionPostCommitYaml = `
name: Test Post Commit
description: set of checks to verify that branch is good
on:
  post-commit:
    branches:
      - feature-*
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for post-commit works
    properties:
      url: "{{.URL}}/post-commit"
`

const actionPostMergeYaml = `
name: Test Post Merge
description: set of checks to verify that branch is good
on:
  post-merge:
    branches:
      - main
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for post-merge works
    properties:
      url: "{{.URL}}/post-merge"
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

	parseAndUpload(t, ctx, repo, branch, actionPreMergeYaml, "action-pre-merge", "testing_pre_merge")
	parseAndUpload(t, ctx, repo, branch, actionPreCommitYaml, "action-pre-commit", "testing_pre_commit")
	parseAndUpload(t, ctx, repo, branch, actionPostCommitYaml, "action-post-commit", "testing_post_commit")
	parseAndUpload(t, ctx, repo, branch, actionPostMergeYaml, "action-post-merge", "testing_post_merge")

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
	var preCommitEvent, postCommitEvent, preMergeEvent, postMergeEvent webhookEventInfo
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
	require.Equal(t, webhookData.queryParams["check_env_vars"], []string{"this_is_actions_var"})

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
	require.Equal(t, branch, postCommitEvent.SourceRef)
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
	require.Equal(t, branch, postMergeEvent.SourceRef)

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

func parseAndUpload(t *testing.T, ctx context.Context, repo, branch, yaml, templateName, actionPath string) {
	t.Helper()

	// render actions based on templates
	docData := struct {
		URL string
	}{
		URL: server.BaseURL(),
	}

	var doc bytes.Buffer

	actionPreMergeTmpl := template.Must(template.New(templateName).Parse(yaml))
	err := actionPreMergeTmpl.Execute(&doc, docData)
	require.NoError(t, err)
	action := doc.String()

	resp, err := uploadContent(ctx, repo, branch, "_lakefs_actions/"+actionPath, action)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())
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
