package esti

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"text/template"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var actionPreCommitTmpl = template.Must(template.New("action-pre-commit").Parse(
	`
name: Test Commit
description: set of checks to verify that branch is good
on:
  pre-commit:
    branches:
      - feature-*
hooks:
  - id: test_webhook
    type: webhook
    properties:
      url: "{{.URL}}/{{.Path}}"
      timeout : {{.Timeout}}
`))

var actionPreCreateBranchTmpl = template.Must(template.New("action-pre-create-branch").Parse(
	`
name: Test Create Branch
description: set of checks to verify that branch is good
on:
  pre-create-branch:
    branches:
hooks:
  - id: test_webhook
    type: webhook
    properties:
      url: "{{.URL}}/{{.Path}}"
      timeout : {{.Timeout}}
`))

var LuaActionFailTmpl = template.Must(template.New("lua-action-fail").Parse(`name: Test Lua {{.EventType}} Fail
on:
  {{.EventType}}:
    branches:
      - {{.Branch}}
hooks:
  - id: lua_{{.HookID}}_fail
    type: lua
    properties:
      script: error("{{.EventType}} lua hook failed intentionally")
`))

func WebhookHooksFailureTest(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface) {
	t.Run("timeout", func(t *testing.T) {
		webhookHooksTimeoutTest(ctx, t, repo, lakeFSClient)
	})
	t.Run("commit fail", func(t *testing.T) {
		webhookHooksCommitFailTest(ctx, t, repo, lakeFSClient)
	})
	t.Run("create branch fail", func(t *testing.T) {
		webhookHooksCreateBranchFailTest(ctx, t, repo, lakeFSClient)
	})
}

func LuaHooksFailureTest(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface) {
	t.Run("merge fail", func(t *testing.T) {
		luaHooksMergeFailTest(ctx, t, repo, lakeFSClient)
	})
}

func createAction(t *testing.T, ctx context.Context, repo, branch, path string, tmp *template.Template, server *WebhookServer, lakeFSClient apigen.ClientWithResponsesInterface) {
	t.Helper()

	// render actions based on templates
	docData := struct {
		URL     string
		Path    string
		Timeout string
	}{
		URL:     server.BaseURL(),
		Path:    path,
		Timeout: hooksTimeout.String(),
	}

	var doc bytes.Buffer
	doc.Reset()
	err := tmp.Execute(&doc, docData)
	require.NoError(t, err)
	content := doc.String()
	actionPath := "_lakefs_actions/" + uuid.NewString()
	uploadResp, err := UploadContent(ctx, repo, branch, actionPath, content, lakeFSClient)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode())
	t.Logf("Uploaded action %s to %s", path, actionPath)
}

func webhookHooksCommitFailHelper(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface, path, branch string) {
	t.Helper()
	server := StartWebhookServer(t)
	defer func() { _ = server.Server().Shutdown(ctx) }()

	t.Log("Create branch", branch)
	resp, err := lakeFSClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	ref := string(resp.Body)
	t.Log("Branch created", ref)

	t.Logf("Upload %s action", path)
	createAction(t, ctx, repo, branch, path, actionPreCommitTmpl, server, lakeFSClient)

	t.Log("Commit - expect failure")
	commitResp, err := lakeFSClient.CommitWithResponse(ctx, repo, branch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, commitResp.StatusCode())
	require.Nil(t, commitResp.JSON201)
}

func webhookHooksTimeoutTest(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface) {
	webhookHooksCommitFailHelper(ctx, t, repo, lakeFSClient, "timeout", "feature-1")
}

func webhookHooksCommitFailTest(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface) {
	webhookHooksCommitFailHelper(ctx, t, repo, lakeFSClient, "fail", "feature-2")
}

func webhookHooksCreateBranchFailTest(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface) {
	const branch = "feature-3"
	server := StartWebhookServer(t)
	defer func() { _ = server.Server().Shutdown(ctx) }()

	t.Log("Create branch", branch)
	resp, err := lakeFSClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	ref := string(resp.Body)
	t.Log("Branch created", ref)

	t.Log("Upload fail action", branch)
	createAction(t, ctx, repo, branch, "fail", actionPreCreateBranchTmpl, server, lakeFSClient)

	t.Log("Create branch - expect failure", "test_branch")
	resp, err = lakeFSClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   "test_branch",
		Source: branch,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode())
}

func luaHooksMergeFailTest(ctx context.Context, t *testing.T, repo string, lakeFSClient apigen.ClientWithResponsesInterface) {
	const branch = "lua-merge-fail-branch"

	t.Log("Create branch", branch)
	createBranchResp, err := lakeFSClient.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
	t.Log("Branch created", ref)

	t.Log("Upload Lua action that will fail on pre-merge")
	action := struct {
		EventType string
		HookID    string
		Branch    string
	}{"pre-merge", "pre_merge", mainBranch}
	var buf bytes.Buffer
	err = LuaActionFailTmpl.Execute(&buf, action)
	require.NoError(t, err)
	actionPath := "_lakefs_actions/action_pre_merge_fail_lua.yaml"
	// Upload action file directly to the branch (not mainBranch) because actions are only triggered once
	// committed, not just staged.
	// This test uploads the action file and a test file to a branch, then merges it into mainBranch.
	// Merge-related action run on mainBranch.
	// Adding the action file to mainBranch via the merge triggers the mainBranch action.
	// Otherwise, if the action file was uploaded directly to the mainBranch in this test,
	// it would remain uncommitted, so won't run.
	resp, err := UploadContent(ctx, repo, branch, actionPath, buf.String(), lakeFSClient)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	WaitForCacheExpiration(ctx, t)

	t.Log("Upload test file")
	resp, err = UploadContent(ctx, repo, branch, "test-file.txt", "test content", lakeFSClient)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	t.Log("Commit the test file")
	commitResp, err := lakeFSClient.CommitWithResponse(ctx, repo, branch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Add test file",
	})
	require.NoError(t, err, "failed to commit")
	require.NotNil(t, commitResp.JSON201)

	commitID := commitResp.JSON201.Id
	t.Log("Commit created successfully", commitID)

	t.Log("Merge - expect failure due to Lua hook error")
	mergeResp, err := lakeFSClient.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, apigen.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, mergeResp.StatusCode(), "merge should fail due to Lua hook error")
	require.Nil(t, mergeResp.JSON200, "merge should not succeed")
}
