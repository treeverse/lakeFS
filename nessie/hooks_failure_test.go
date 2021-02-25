package nessie

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"text/template"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/models"
)

const actionPreCommitTemplate = `
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
`

func TestHooksTimeout(t *testing.T) {
	hookFailToCommit(t, "timeout")
}

func TestHooksFail(t *testing.T) {
	hookFailToCommit(t, "fail")
}

func hookFailToCommit(t *testing.T, path string) {
	t.Helper()
	ctx, logger, repo := setupTest(t)
	const branch = "feature-1"

	logger.WithField("branch", branch).Info("Create branch")
	ref, err := client.Branches.CreateBranch(
		branches.NewCreateBranchParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(&models.BranchCreation{
				Name:   swag.String(branch),
				Source: swag.String(masterBranch),
			}), nil)
	require.NoError(t, err, "failed to create branch")
	logger.WithField("branchRef", ref).Info("Branch created")
	logger.WithField("branch", branch).Info("Upload initial content")

	// render actions based on templates
	docData := struct {
		URL  string
		Path string
	}{
		URL:  fmt.Sprintf("http://nessie:%d", server.port),
		Path: path,
	}

	var doc bytes.Buffer
	actionPreCommitTmpl := template.Must(template.New("action-pre-commit").Parse(actionPreCommitTemplate))
	doc.Reset()
	err = actionPreCommitTmpl.Execute(&doc, docData)
	require.NoError(t, err)
	preCommitAction := doc.String()

	_, err = client.Objects.UploadObject(
		objects.NewUploadObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(branch).
			WithPath("_lakefs_actions/testing_pre_commit").
			WithContent(runtime.NamedReader("content", strings.NewReader(preCommitAction))), nil)
	require.NoError(t, err)
	logger.WithField("branch", branch).Info("Commit initial content")

	stats, err := client.Commits.Commit(
		commits.NewCommitParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(branch).
			WithCommit(&models.CommitCreation{Message: swag.String("Initial content")}),
		nil)
	require.Error(t, err, "commit should fail due to webhook")
	require.Nil(t, stats)
	require.Contains(t, err.Error(), "412")
}
