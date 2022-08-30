package esti

import (
	"bytes"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
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

const hooksTimeout = 2 * time.Second

func TestHooksTimeout(t *testing.T) {
	hookFailToCommit(t, "timeout")
}

func TestHooksFail(t *testing.T) {
	hookFailToCommit(t, "fail")
}

func hookFailToCommit(t *testing.T, path string) {
	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	const branch = "feature-1"

	logger.WithField("branch", branch).Info("Create branch")
	resp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	ref := string(resp.Body)
	logger.WithField("branchRef", ref).Info("Branch created")
	logger.WithField("branch", branch).Info("Upload initial content")

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
	err = actionPreCommitTmpl.Execute(&doc, docData)
	require.NoError(t, err)
	preCommitAction := doc.String()

	uploadResp, err := uploadContent(ctx, repo, branch, "_lakefs_actions/testing_pre_commit", preCommitAction)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode())
	logger.WithField("branch", branch).Info("Commit initial content")

	commitResp, err := client.CommitWithResponse(ctx, repo, branch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "Initial content",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, commitResp.StatusCode())
	require.Nil(t, commitResp.JSON201)
}
