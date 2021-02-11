package actions_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/actions"
)

func TestManager_RunActions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "OK")
	}))
	defer ts.Close()

	testOutputWriter := &MockOutputWriter{}
	testOutputWriter.On("OutputWrite", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	testSource := &MockSource{}
	testSource.On("List").Return([]string{"act.yaml"}, nil)
	testSource.On("Load", "act.yaml").Return([]byte(`
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
		Source:        testSource,
		Output:        testOutputWriter,
		EventType:     actions.EventTypePreCommit,
		EventTime:     evtTime,
		RepositoryID:  "repoID",
		BranchID:      "branchID",
		SourceRef:     "sourceRef",
		CommitMessage: "commitMessage",
		Committer:     "committer",
		Metadata:      map[string]string{"key": "value"},
	}
	ctx := context.Background()
	actionsManager := actions.NewManager(nil)
	err := actionsManager.RunActions(ctx, evt)
	require.NoError(t, err)
	testSource.AssertExpectations(t)
	testOutputWriter.AssertExpectations(t)
}
