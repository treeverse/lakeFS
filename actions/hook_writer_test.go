package actions_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/actions"
)

type MyOutputWriter struct {
	mock.Mock
}

func (m *MyOutputWriter) OutputWrite(ctx context.Context, name string, reader io.Reader) error {
	args := m.Mock.Called(ctx, name, reader)
	return args.Error(0)
}

func TestHookWriter_OutputWritePath(t *testing.T) {
	ctx := context.Background()
	contentReader := strings.NewReader("content")

	hookOutput := actions.FormatHookOutputPath("runID", "actionName", "hookID", "name1")
	writer := &MyOutputWriter{}
	writer.On("OutputWrite", ctx, hookOutput, contentReader).Return(nil)

	w := &actions.HookWriter{
		RunID:      "runID",
		ActionName: "actionName",
		HookID:     "hookID",
		Writer:     writer,
	}
	err := w.OutputWrite(ctx, "name1", contentReader)
	require.NoError(t, err)
	writer.AssertExpectations(t)
}

func TestHookWriter_OutputWriteError(t *testing.T) {
	errSomeError := errors.New("some error")
	writer := &MyOutputWriter{}
	writer.On("OutputWrite", mock.Anything, mock.Anything, mock.Anything).Return(errSomeError)

	w := &actions.HookWriter{
		RunID:      "runID",
		ActionName: "actionName",
		HookID:     "hookID",
		Writer:     writer,
	}
	ctx := context.Background()
	contentReader := strings.NewReader("content")
	err := w.OutputWrite(ctx, "name1", contentReader)
	require.True(t, errors.Is(err, errSomeError))
	writer.AssertExpectations(t)
}
