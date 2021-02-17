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
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/actions/mock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestServiceRun(t *testing.T) {
	conn, _ := testutil.GetDB(t, databaseURI)

	now := time.Now()
	evt := actions.Event{
		Type:          actions.EventTypePreCommit,
		Time:          now,
		RepositoryID:  "repoID",
		BranchID:      "branchID",
		SourceRef:     "sourceRef",
		CommitMessage: "commitMessage",
		Committer:     "committer",
		Metadata:      map[string]string{"key": "value"},
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
		if eventInfo.EventType != string(evt.Type) {
			t.Errorf("Webhook post EventType=%s, expected=%s", eventInfo.EventType, evt.Type)
		}
		if eventInfo.ActionName != actionName {
			t.Errorf("Webhook post ActionName=%s, expected=%s", eventInfo.ActionName, actionName)
		}
		if eventInfo.HookID != hookID {
			t.Errorf("Webhook post HookID=%s, expected=%s", eventInfo.HookID, hookID)
		}
		if eventInfo.RepositoryID != evt.RepositoryID {
			t.Errorf("Webhook post RepositoryID=%s, expected=%s", eventInfo.RepositoryID, evt.RepositoryID)
		}
		if eventInfo.BranchID != evt.BranchID {
			t.Errorf("Webhook post BranchID=%s, expected=%s", eventInfo.BranchID, evt.BranchID)
		}
		if eventInfo.SourceRef != evt.SourceRef {
			t.Errorf("Webhook post SourceRef=%s, expected=%s", eventInfo.SourceRef, evt.SourceRef)
		}
		if eventInfo.CommitMessage != evt.CommitMessage {
			t.Errorf("Webhook post CommitMessage=%s, expected=%s", eventInfo.CommitMessage, evt.CommitMessage)
		}
		if eventInfo.Committer != evt.Committer {
			t.Errorf("Webhook post Committer=%s, expected=%s", eventInfo.Committer, evt.Committer)
		}
		if diff := deep.Equal(eventInfo.Metadata, evt.Metadata); diff != nil {
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
	testOutputWriter.EXPECT().OutputWrite(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	testSource := mock.NewMockSource(ctrl)
	testSource.EXPECT().List(gomock.Any()).Return([]actions.FileRef{{Path: "act.yaml", Address: "act.addr"}}, nil)
	testSource.EXPECT().Load(gomock.Any(), actions.FileRef{Path: "act.yaml", Address: "act.addr"}).Return([]byte(actionContent), nil)

	deps := actions.Deps{
		Source: testSource,
		Output: testOutputWriter,
	}

	// run actions
	actionsService := actions.NewService(conn)
	runID, err := actionsService.Run(ctx, evt, deps)
	if err != nil {
		t.Fatalf("Run() failed with err=%s", err)
	}
	if len(runID) == 0 {
		t.Fatal("Run() should return a value for run ID")
	}

	// update commit id
	err = actionsService.UpdateCommitID(ctx, evt.RepositoryID, runID, "commit1")
	if err != nil {
		t.Fatalf("UpdateCommitID() failed with err=%s", err)
	}

	// get existing run
	runResult, err := actionsService.GetRun(ctx, evt.RepositoryID, runID)
	if err != nil {
		t.Fatal("GetRun() get run result", err)
	}
	if runResult.RunID != runID {
		t.Errorf("GetRun() result RunID=%s, expect=%s", runResult.RunID, runID)
	}
	if runResult.BranchID != evt.BranchID {
		t.Errorf("GetRun() result BranchID=%s, expect=%s", runResult.BranchID, evt.BranchID)
	}
	if runResult.EventType != string(evt.Type) {
		t.Errorf("GetRun() result Type=%s, expect=%s", runResult.EventType, evt.Type)
	}
	startTime := runResult.StartTime.Round(time.Second)
	expectedStartTime := evt.Time.Round(time.Second)
	if startTime != expectedStartTime {
		t.Errorf("GetRun() result StartTime=%s, expect=%s", startTime, expectedStartTime)
	}
	if runResult.EndTime.Sub(runResult.StartTime) < 0 {
		t.Error("GetRun() result EndTime-StartTime can't be negative")
	}
	const expectedPassed = true
	if runResult.Passed != expectedPassed {
		t.Errorf("GetRun() result Passed=%t, expect=%t", runResult.Passed, expectedPassed)
	}
	const expectedCommitID = "commit1"
	if runResult.CommitID != expectedCommitID {
		t.Errorf("GetRun() result CommitID=%s, expect=%s", runResult.CommitID, expectedCommitID)
	}
	// get run - not found
	runResult, err = actionsService.GetRun(ctx, evt.RepositoryID, "billing")
	if !errors.Is(err, db.ErrNotFound) {
		t.Errorf("GetRun() err=%v, expected=%v", err, db.ErrNotFound)
	}
	if runResult != nil {
		t.Errorf("GetRun() result=%v, expected nil", runResult)
	}
}
