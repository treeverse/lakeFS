package nessie

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
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
const actionPreCreateTagYaml = `
name: Test Pre Create Tag
description: set of checks to verify that branch is good
on:
  pre-create-tag:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for pre-create tag works
    properties:
      url: "{{.URL}}/pre-create-tag"
`
const actionPostCreateTagYaml = `
name: Test Post Create Tag
description: set of checks to verify that branch is good
on:
  post-create-tag:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for post-create tag works
    properties:
      url: "{{.URL}}/post-create-tag"
`

const actionPreDeleteTagYaml = `
name: Test Pre Delete Tag
description: set of checks to verify that branch is good
on:
  pre-delete-tag:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for pre-delete tag works
    properties:
      url: "{{.URL}}/pre-delete-tag"
`
const actionPostDeleteTagYaml = `
name: Test Post Delete Tag
description: set of checks to verify that branch is good
on:
  post-delete-tag:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for post-delete tag works
    properties:
      url: "{{.URL}}/post-delete-tag"
`
const actionPreCreateBranchYaml = `
name: Test Pre Create Branch
description: set of checks to verify that branch is good
on:
  pre-create-branch:
    branches:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for pre-create branch works
    properties:
      url: "{{.URL}}/pre-create-branch"
`
const actionPostCreateBranchYaml = `
name: Test Post Create Branch
description: set of checks to verify that branch is good
on:
  post-create-branch:
    branches:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for post-create branch works
    properties:
      url: "{{.URL}}/post-create-branch"
`

const actionPreDeleteBranchYaml = `
name: Test Pre Delete Branch
description: set of checks to verify that branch is good
on:
  pre-delete-branch:
    branches:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for pre-delete branch works
    properties:
      url: "{{.URL}}/pre-delete-branch"
`
const actionPostDeleteBranchYaml = `
name: Test Post Delete Branch
description: set of checks to verify that branch is good
on:
  post-delete-branch:
    branches:
hooks:
  - id: test_webhook
    type: webhook
    description: Check webhooks for post-delete branch works
    properties:
      url: "{{.URL}}/post-delete-branch"
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
	parseAndUpload(t, ctx, repo, branch, actionPreCreateTagYaml, "action-pre-create-tag", "testing_pre_create_tag")
	parseAndUpload(t, ctx, repo, branch, actionPostCreateTagYaml, "action-post-create-tag", "testing_post_create_tag")
	parseAndUpload(t, ctx, repo, branch, actionPreDeleteTagYaml, "action-pre-delete-tag", "testing_pre_delete_tag")
	parseAndUpload(t, ctx, repo, branch, actionPostDeleteTagYaml, "action-post-delete-tag", "testing_post_delete_tag")
	parseAndUpload(t, ctx, repo, branch, actionPreCreateBranchYaml, "action-pre-create-branch", "testing_pre_create_branch")
	parseAndUpload(t, ctx, repo, branch, actionPostCreateBranchYaml, "action-post-create-branch", "testing_post_create_branch")
	parseAndUpload(t, ctx, repo, branch, actionPreDeleteBranchYaml, "action-pre-delete-branch", "testing_pre_delete_branch")
	parseAndUpload(t, ctx, repo, branch, actionPostDeleteBranchYaml, "action-post-delete-branch", "testing_post_delete_branch")

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
	require.Equal(t, strings.Join(webhookData.queryParams["check_env_vars"], ""), `"this_is_actions_var"`)

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

	// Test create / delete tag hooks
	const tagID = "tag_test_hooks"
	var preCreateTagEvent, postCreateTagEvent, preDeleteTagEvent, postDeleteTagEvent webhookEventInfo
	createTagResp, err := client.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
		Id:  tagID,
		Ref: commitRecord.Id,
	})

	require.NoError(t, err, "failed to create tag")
	require.Equal(t, http.StatusCreated, createTagResp.StatusCode())

	// Testing pre-create tag hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre create tag")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&preCreateTagEvent), "reading pre-create tag data")
	require.Equal(t, "pre-create-tag", preCreateTagEvent.EventType)
	require.Equal(t, "Test Pre Create Tag", preCreateTagEvent.ActionName)
	require.Equal(t, "test_webhook", preCreateTagEvent.HookID)
	require.Equal(t, repo, preCreateTagEvent.RepositoryID)
	require.Equal(t, commitRecord.Id, preCreateTagEvent.CommitID)
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
	require.Equal(t, commitRecord.Id, postCreateTagEvent.CommitID)
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
	require.Equal(t, commitRecord.Id, preDeleteTagEvent.SourceRef)
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
	require.Equal(t, commitRecord.Id, postDeleteTagEvent.SourceRef)
	require.Equal(t, tagID, postDeleteTagEvent.TagID)

	// Test create / delete branch hooks
	var preCreateBranchEvent, postCreateBranchEvent, preDeleteBranchEvent, postDeleteBranchEvent webhookEventInfo
	_, _ = preDeleteBranchEvent, postDeleteBranchEvent
	const testBranch = "test_branch_delete"
	createBranchResp, err = client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   testBranch,
		Source: branch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())

	// Testing pre-create branch hook response
	webhookData, err = responseWithTimeout(server, 1*time.Minute)
	require.NoError(t, err)

	require.NoError(t, webhookData.err, "error on pre create branch")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&preCreateBranchEvent), "reading pre-create branch data")
	require.Equal(t, "pre-create-branch", preCreateBranchEvent.EventType)
	require.Equal(t, "Test Pre Create Branch", preCreateBranchEvent.ActionName)
	require.Equal(t, "test_webhook", preCreateBranchEvent.HookID)
	require.Equal(t, repo, preCreateBranchEvent.RepositoryID)
	require.Equal(t, branch, preCreateBranchEvent.SourceRef)
	require.Equal(t, testBranch, preCreateBranchEvent.BranchID)
	require.Equal(t, commitRecord.Id, preCreateBranchEvent.CommitID)

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
	require.Equal(t, branch, postCreateBranchEvent.SourceRef)
	require.Equal(t, testBranch, postCreateBranchEvent.BranchID)
	require.Equal(t, commitRecord.Id, postCreateBranchEvent.CommitID)

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
	TagID         string            `json:"tag_id"`
	CommitID      string            `json:"commit_id"`
	CommitMessage string            `json:"commit_message"`
	Committer     string            `json:"committer"`
	Metadata      map[string]string `json:"metadata"`
}
