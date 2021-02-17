package actions_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/actions/mock"
)

func TestHookWriter_OutputWritePath(t *testing.T) {
	ctx := context.Background()
	content := "content"
	contentReader := strings.NewReader(content)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runID := actions.NewRunID()
	hookOutput := actions.FormatHookOutputPath(runID, "actionName", "hookID")
	writer := mock.NewMockOutputWriter(ctrl)
	writer.EXPECT().OutputWrite(ctx, hookOutput, contentReader, int64(len(content))).Return(nil)

	w := &actions.HookOutputWriter{
		RunID:      runID,
		ActionName: "actionName",
		HookID:     "hookID",
		Writer:     writer,
	}
	err := w.OutputWrite(ctx, contentReader, int64(len(content)))
	if err != nil {
		t.Fatalf("OutputWrite failed with err=%s", err)
	}
}

func TestHookWriter_OutputWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errSomeError := errors.New("some error")
	writer := mock.NewMockOutputWriter(ctrl)
	writer.EXPECT().OutputWrite(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errSomeError)

	w := &actions.HookOutputWriter{
		RunID:      actions.NewRunID(),
		ActionName: "actionName",
		HookID:     "hookID",
		Writer:     writer,
	}
	ctx := context.Background()
	contentReader := strings.NewReader("content")
	err := w.OutputWrite(ctx, contentReader, 10)
	if !errors.Is(err, errSomeError) {
		t.Fatalf("OutputWrite() err=%v expected=%v", err, errSomeError)
	}
}
