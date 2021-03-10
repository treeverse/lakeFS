package actions_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestServiceRun(t *testing.T) {
	conn, _ := testutil.GetDB(t, databaseURI)

	record := graveler.HookRecord{
		RunID:            graveler.NewRunID(),
		EventType:        graveler.EventTypePreCommit,
		StorageNamespace: "storageNamespace",
		RepositoryID:     "repoID",
		BranchID:         "branchID",
		SourceRef:        "sourceRef",
		Commit: graveler.Commit{
			Message:   "commitMessage",
			Committer: "committer",
			Metadata:  map[string]string{"key": "value"},
		},
	}
	const actionName = "test action"
	const hookID = "hook_id"
	hookResponse := "OK"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error("Failed to read webhook post data", err)
			return
		}

		queryParams := map[string][]string(r.URL.Query())
		require.Len(t, queryParams["prefix"], 1)
		require.Equal(t, "public/", queryParams["prefix"][0])
		require.Len(t, queryParams["disallow"], 2)
		require.Equal(t, "user_", queryParams["disallow"][0])
		require.Equal(t, "private_", queryParams["disallow"][1])

		var eventInfo actions.WebhookEventInfo
		err = json.Unmarshal(data, &eventInfo)
		if err != nil {
			t.Error("Failed to unmarshal webhook data", err)
			return
		}
		if eventInfo.EventType != string(record.EventType) {
			t.Errorf("Webhook post EventType=%s, expected=%s", eventInfo.EventType, record.EventType)
		}
		if eventInfo.ActionName != actionName {
			t.Errorf("Webhook post ActionName=%s, expected=%s", eventInfo.ActionName, actionName)
		}
		if eventInfo.HookID != hookID {
			t.Errorf("Webhook post HookID=%s, expected=%s", eventInfo.HookID, hookID)
		}
		if eventInfo.RepositoryID != record.RepositoryID.String() {
			t.Errorf("Webhook post RepositoryID=%s, expected=%s", eventInfo.RepositoryID, record.RepositoryID)
		}
		if eventInfo.BranchID != record.BranchID.String() {
			t.Errorf("Webhook post BranchID=%s, expected=%s", eventInfo.BranchID, record.BranchID)
		}
		if eventInfo.SourceRef != record.SourceRef.String() {
			t.Errorf("Webhook post SourceRef=%s, expected=%s", eventInfo.SourceRef, record.SourceRef)
		}
		if eventInfo.CommitMessage != record.Commit.Message {
			t.Errorf("Webhook post CommitMessage=%s, expected=%s", eventInfo.CommitMessage, record.Commit.Message)
		}
		if eventInfo.Committer != record.Commit.Committer {
			t.Errorf("Webhook post Committer=%s, expected=%s", eventInfo.Committer, record.Commit.Committer)
		}
		if diff := deep.Equal(eventInfo.CommitMetadata, map[string]string(record.Commit.Metadata)); diff != nil {
			t.Errorf("Webhook post Metadata diff=%s", diff)
		}
		_, _ = io.WriteString(w, hookResponse)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	actionContent := `name: ` + actionName + `
on:
  pre-commit: {}
hooks:
  - id: ` + hookID + `
    type: webhook
    properties:
      url: "` + ts.URL + `/hook"
      timeout: 2m30s
      query_params:
        prefix: public/
        disallow: ["user_", "private_"]
`

	ctx := context.Background()
	testOutputWriter := mock.NewMockOutputWriter(ctrl)
	expectedHookRunID := actions.NewHookRunID(0, 0)
	var lastManifest *actions.RunManifest
	var writerBytes []byte
	testOutputWriter.EXPECT().
		OutputWrite(ctx, record.StorageNamespace.String(), actions.FormatHookOutputPath(record.RunID, expectedHookRunID), gomock.Any(), gomock.Any()).
		Return(nil).
		DoAndReturn(func(ctx context.Context, storageNamespace, name string, reader io.Reader, size int64) error {
			var err error
			writerBytes, err = ioutil.ReadAll(reader)
			return err
		})
	testOutputWriter.EXPECT().
		OutputWrite(ctx, record.StorageNamespace.String(), actions.FormatRunManifestOutputPath(record.RunID), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, storageNamespace, name string, reader io.Reader, size int64) error {
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				return err
			}
			var manifest actions.RunManifest
			err = json.Unmarshal(data, &manifest)
			if err != nil {
				return err
			}
			lastManifest = &manifest
			return nil
		}).
		Times(2)

	testSource := mock.NewMockSource(ctrl)
	testSource.EXPECT().
		List(ctx, record).
		Return([]string{"act.yaml"}, nil)
	testSource.EXPECT().
		Load(ctx, record, "act.yaml").
		Return([]byte(actionContent), nil)

	// run actions
	now := time.Now()
	actionsService := actions.NewService(conn, testSource, testOutputWriter)

	err := actionsService.Run(ctx, record)
	if err != nil {
		t.Fatalf("Run() failed with err=%s", err)
	}
	if lastManifest == nil {
		t.Fatalf("Run() should store manifest")
	}
	if lastManifest.Run.RunID != record.RunID {
		t.Errorf("Run() manifest RunID %s, expected %s", lastManifest.Run.RunID, record.RunID)
	}
	if lastManifest.Run.CommitID != "" {
		t.Errorf("Run() manifest CommitID %s, expected empty", lastManifest.Run.CommitID)
	}
	lastManifest = nil

	// update commit using post event record
	err = actionsService.UpdateCommitID(ctx, record.RepositoryID.String(), record.StorageNamespace.String(), record.RunID, "commit1")
	if err != nil {
		t.Fatalf("UpdateCommitID() failed with err=%s", err)
	}
	if lastManifest == nil {
		t.Fatalf("UpdateCommitID() should store updated manifest")
	}
	if lastManifest.Run.RunID != record.RunID {
		t.Errorf("UpdateCommitID() manifest RunID %s, expected %s", lastManifest.Run.RunID, record.RunID)
	}
	if lastManifest.Run.CommitID != "commit1" {
		t.Errorf("UpdateCommitID() manifest CommitID %s, expected 'commit1'", lastManifest.Run.CommitID)
	}

	// get run result
	runResult, err := actionsService.GetRunResult(ctx, record.RepositoryID.String(), record.RunID)
	if err != nil {
		t.Fatal("GetRunResult() get run result", err)
	}
	if runResult.RunID != record.RunID {
		t.Errorf("GetRunResult() result RunID=%s, expect=%s", runResult.RunID, record.RunID)
	}
	if runResult.BranchID != record.BranchID.String() {
		t.Errorf("GetRunResult() result BranchID=%s, expect=%s", runResult.BranchID, record.BranchID)
	}
	if runResult.EventType != string(record.EventType) {
		t.Errorf("GetRunResult() result Type=%s, expect=%s", runResult.EventType, record.EventType)
	}
	startTime := runResult.StartTime
	if startTime.Before(now) {
		t.Errorf("GetRunResult() result StartTime should be after we run the actions, %v > %v", startTime, now)
	}
	endTime := runResult.EndTime
	if endTime.Before(startTime) {
		t.Errorf("GetRunResult() result EndTime should be same or after StartTime %v >= %v", endTime, startTime)
	}

	const expectedPassed = true
	if runResult.Passed != expectedPassed {
		t.Errorf("GetRunResult() result Passed=%t, expect=%t", runResult.Passed, expectedPassed)
	}
	const expectedCommitID = "commit1"
	if runResult.CommitID != expectedCommitID {
		t.Errorf("GetRunResult() result CommitID=%s, expect=%s", runResult.CommitID, expectedCommitID)
	}

	// get run - not found
	runResult, err = actionsService.GetRunResult(ctx, record.RepositoryID.String(), "not-run-id")
	expectedErr := actions.ErrNotFound
	if !errors.Is(err, expectedErr) {
		t.Errorf("GetRunResult() err=%v, expected=%v", err, expectedErr)
	}
	if runResult != nil {
		t.Errorf("GetRunResult() result=%v, expected nil", runResult)
	}

	require.Greater(t, bytes.Count(writerBytes, []byte("\n")), 10)
}
