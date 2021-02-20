package actions_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/actions/mock"
)

func TestManager_RunActions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "OK")
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	testOutputWriter := mock.NewMockOutputWriter(ctrl)
	testOutputWriter.EXPECT().OutputWrite(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	testSource := mock.NewMockSource(ctrl)
	testSource.EXPECT().List(gomock.Any()).Return([]actions.FileRef{{Path: "act.yaml", Address: "act.addr"}}, nil)
	testSource.EXPECT().Load(gomock.Any(), actions.FileRef{Path: "act.yaml", Address: "act.addr"}).Return([]byte(`
name: test action
on:
  pre-commit: {}
hooks:
  - id: hook_id
    type: webhook
    properties:
      url: "`+ts.URL+`/hook"
`), nil)

	evtTime := time.Now()
	evt := actions.Event{
		EventType:     actions.EventTypePreCommit,
		EventTime:     evtTime,
		RepositoryID:  "repoID",
		BranchID:      "branchID",
		SourceRef:     "sourceRef",
		CommitMessage: "commitMessage",
		Committer:     "committer",
		Metadata:      map[string]string{"key": "value"},
	}
	deps := actions.Deps{
		Source: testSource,
		Output: testOutputWriter,
	}
	actionsManager := actions.New(nil)
	err := actionsManager.Run(ctx, evt, deps)
	if err != nil {
		t.Fatalf("Run() failed with err=%s", err)
	}
}
