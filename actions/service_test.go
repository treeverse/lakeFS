package actions_test

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/actions/mock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestServiceRun(t *testing.T) {
	conn, _ := testutil.GetDB(t, databaseURI)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error("Failed to read webhook post data", err)
		} else {
			_, _ = w.Write(data)
		}
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const actionName = "test action"
	actionContent := `name: ` + actionName + `
on:
  pre-commit: {}
hooks:
  - id: hook_id
    type: webhook
    properties:
      url: "` + ts.URL + `/hook"
`

	now := time.Now()
	evt := actions.Event{
		RunID:         actions.NewRunID(),
		Type:          actions.EventTypePreCommit,
		Time:          now,
		RepositoryID:  "repoID",
		BranchID:      "branchID",
		SourceRef:     "sourceRef",
		CommitMessage: "commitMessage",
		Committer:     "committer",
		Metadata:      map[string]string{"key": "value"},
	}

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
	err := actionsService.Run(ctx, evt, deps)
	if err != nil {
		t.Fatalf("Run() failed with err=%s", err)
	}

	// update commit id
	err = actionsService.UpdateCommitID(ctx, evt.RepositoryID, evt.RunID, evt.Type, "commit1")
	if err != nil {
		t.Fatalf("UpdateCommitID() failed with err=%s", err)
	}

	// get existing run
	runResult, err := actionsService.GetRun(ctx, evt.RepositoryID, evt.RunID, evt.Type)
	if err != nil {
		t.Fatal("GetRun() get run result", err)
	}
	if runResult.RunID != evt.RunID {
		t.Errorf("GetRun() result RunID=%s, expect=%s", runResult.RunID, evt.RunID)
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
	// get run - not found
	runResult, err = actionsService.GetRun(ctx, evt.RepositoryID, evt.RunID, "billing")
	if !errors.Is(err, db.ErrNotFound) {
		t.Errorf("GetRun() err=%v, expected=%v", err, db.ErrNotFound)
	}
	if runResult != nil {
		t.Errorf("GetRun() result=%v, expected nil", runResult)
	}

}
