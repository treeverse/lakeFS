package actions_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/treeverse/lakefs/graveler"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/actions/mock"
	"github.com/treeverse/lakefs/testutil"
)

func TestServiceRun(t *testing.T) {
	conn, _ := testutil.GetDB(t, databaseURI)

	record := graveler.HookRecord{
		RunID:        graveler.NewRunID(),
		EventType:    graveler.EventTypePreCommit,
		RepositoryID: "repoID",
		BranchID:     "branchID",
		SourceRef:    "sourceRef",
		Commit: graveler.Commit{
			Message:   "commitMessage",
			Committer: "committer",
			Metadata:  map[string]string{"key": "value"},
		},
	}
	const actionName = "test action"
	const hookID = "hook_id"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error("Failed to read webhook post data", err)
			return
		}
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
		if diff := deep.Equal(eventInfo.Metadata, map[string]string(record.Commit.Metadata)); diff != nil {
			t.Errorf("Webhook post Metadata diff=%s", diff)
		}
		_, _ = io.WriteString(w, "OK")
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
`

	ctx := context.Background()
	testOutputWriter := mock.NewMockOutputWriter(ctrl)
	testOutputWriter.EXPECT().OutputWrite(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	testSource := mock.NewMockSource(ctrl)
	testSource.EXPECT().List(gomock.Any(), gomock.Any()).Return([]string{"act.yaml"}, nil)
	testSource.EXPECT().Load(gomock.Any(), gomock.Any(), "act.yaml").Return([]byte(actionContent), nil)

	// run actions
	actionsService := actions.NewService(conn, testSource, testOutputWriter)
	err := actionsService.Run(ctx, record)
	if err != nil {
		t.Fatalf("Run() failed with err=%s", err)
	}

	// update commit id
	err = actionsService.UpdateCommitID(ctx, record.RepositoryID.String(), record.RunID, "commit1")
	if err != nil {
		t.Fatalf("UpdateCommitID() failed with err=%s", err)
	}

	// get existing run
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
	//startTime := runResult.StartTime.Round(time.Second)
	//expectedStartTime := record.Time.Round(time.Second)
	//if startTime != expectedStartTime {
	//	t.Errorf("GetRunResult() result StartTime=%s, expect=%s", startTime, expectedStartTime)
	//}
	//if runResult.EndTime.Sub(runResult.StartTime) < 0 {
	//	t.Error("GetRunResult() result EndTime-StartTime can't be negative")
	//}
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
}
