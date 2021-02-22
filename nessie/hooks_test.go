package nessie

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"text/template"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/models"
)

const actionPreMergeYaml = `
name: Test Merge
description: set of checks to verify that branch is good
on:
  pre-merge:
    branches:
      - master
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

var (
	actionPreMergeTmpl  = template.Must(template.New("action-pre-merge").Parse(actionPreMergeYaml))
	actionPreCommitTmpl = template.Must(template.New("action-pre-commit").Parse(actionPreCommitYaml))
)

func TestHooks(t *testing.T) {
	server := startWebhookServer(t)

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

	var doc bytes.Buffer
	err = actionPreMergeTmpl.Execute(&doc, server.s)
	require.NoError(t, err)
	preMergeAction := doc.String()

	_, err = client.Objects.UploadObject(
		objects.NewUploadObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(branch).
			WithPath("_lakefs_actions/testing_pre_merge").
			WithContent(runtime.NamedReader("content", strings.NewReader(preMergeAction))), nil)
	require.NoError(t, err)

	doc.Reset()
	err = actionPreCommitTmpl.Execute(&doc, server.s)
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
	require.NoError(t, err, "failed to commit initial content")

	webhookData := <-server.respCh
	require.NoError(t, webhookData.err, "error on pre commit serving")
	decoder := json.NewDecoder(bytes.NewReader(webhookData.data))
	var commitEvent, mergeEvent webhookEventInfo
	require.NoError(t, decoder.Decode(&commitEvent), "reading pre-commit data")

	require.Equal(t, "pre-commit", commitEvent.EventType)
	require.Equal(t, "Test Commit", commitEvent.ActionName)
	require.Equal(t, "test_webhook", commitEvent.HookID)
	require.Equal(t, repo, commitEvent.RepositoryID)
	require.Equal(t, branch, commitEvent.BranchID)
	require.Equal(t, stats.Payload.Committer, commitEvent.Committer)
	require.Equal(t, stats.Payload.Message, commitEvent.CommitMessage)
	require.Equal(t, "", commitEvent.SourceRef)
	require.Equal(t, stats.Payload.Metadata, commitEvent.Metadata)

	mergeRes, err := client.Refs.MergeIntoBranch(
		refs.NewMergeIntoBranchParamsWithContext(ctx).WithRepository(repo).WithDestinationBranch(masterBranch).WithSourceRef(branch), nil)

	webhookData = <-server.respCh
	require.NoError(t, webhookData.err, "failed to merge branches")
	logger.WithField("mergeResult", mergeRes).Info("Merged successfully")

	require.NoError(t, err, "error on pre commit serving")
	decoder = json.NewDecoder(bytes.NewReader(webhookData.data))
	require.NoError(t, decoder.Decode(&mergeEvent), "reading pre-merge data")

	require.Equal(t, "pre-merge", mergeEvent.EventType)
	require.Equal(t, "Test Merge", mergeEvent.ActionName)
	require.Equal(t, "test_webhook", mergeEvent.HookID)
	require.Equal(t, repo, mergeEvent.RepositoryID)
	require.Equal(t, masterBranch, mergeEvent.BranchID)
	require.Equal(t, branch, mergeEvent.SourceRef)
}

type hookResponse struct {
	path string
	err  error
	data []byte
}

type server struct {
	s      *httptest.Server
	respCh chan hookResponse
}

func startWebhookServer(t *testing.T) *server {
	respCh := make(chan hookResponse, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("/pre-commit", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-merge", hookHandlerFunc(respCh))
	ts := httptest.NewServer(mux)
	t.Cleanup(func() {
		ts.Close()
		close(respCh)
	})

	return &server{
		s:      ts,
		respCh: respCh,
	}
}

func hookHandlerFunc(respCh chan hookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			respCh <- hookResponse{path: request.URL.Path, err: err}
			_, _ = io.WriteString(writer, "Failed")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		respCh <- hookResponse{path: request.URL.Path, data: data}
		_, _ = io.WriteString(writer, "OK")
		writer.WriteHeader(http.StatusOK)
		return
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
	CommitMessage string            `json:"commit_message"`
	Committer     string            `json:"committer"`
	Metadata      map[string]string `json:"metadata"`
}
