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

type MySource struct {
	mock.Mock
}

func (m *MySource) List() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *MySource) Load(name string) ([]byte, error) {
	args := m.Called(name)
	return args.Get(0).([]byte), args.Error(1)
}

func TestProcess_Run(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "OK")
	}))
	defer ts.Close()

	t.Run("basic", func(t *testing.T) {
		mySource := &MySource{}
		mySource.On("List").Return([]string{"act.yaml"}, nil)
		mySource.On("Load", "act.yaml").Return([]byte(`
name: test action
on:
  pre-commit: {}
hooks:
  - id: hook_id
    type: webhook
    properties:
      url: "`+ts.URL+`/hook"
`), nil)

		myOutputWriter := &MyOutputWriter{}
		myOutputWriter.On("OutputWrite", mock.Anything, mock.Anything, mock.Anything).Return(nil)

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
		ctx := context.Background()
		err := actions.NewProcess(mySource, myOutputWriter, evt).Run(ctx)
		require.NoError(t, err)
		mySource.AssertExpectations(t)
		myOutputWriter.AssertExpectations(t)
	})
}
