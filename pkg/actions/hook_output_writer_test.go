package actions_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestHookWriter_OutputWritePath(t *testing.T) {
	ctx := context.Background()
	content := "content"
	contentReader := strings.NewReader(content)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const hookID = "hookID"
	const actionName = "actionName"
	const storageNamespace = "storageNamespace"
	hooks := graveler.HooksNoOp{}
	runID := hooks.NewRunID()
	hookRunID := hooks.NewRunID()
	writer := mock.NewMockOutputWriter(ctrl)
	writer.EXPECT().OutputWrite(ctx, storageNamespace, actions.FormatHookOutputPath(runID, hookRunID), contentReader, int64(len(content))).Return(nil)

	w := &actions.HookOutputWriter{
		StorageNamespace: storageNamespace,
		RunID:            runID,
		HookID:           hookID,
		HookRunID:        hookRunID,
		ActionName:       actionName,
		Writer:           writer,
	}
	err := w.OutputWrite(ctx, contentReader, int64(len(content)))
	if err != nil {
		t.Fatalf("OutputWrite failed with err=%s", err)
	}
}

func TestHookWriter_OutputWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	hooks := graveler.HooksNoOp{}
	runID := hooks.NewRunID()
	hookRunID := hooks.NewRunID()
	errSomeError := errors.New("some error")
	writer := mock.NewMockOutputWriter(ctrl)
	writer.EXPECT().OutputWrite(ctx, "storageNamespace", actions.FormatHookOutputPath(runID, hookRunID), gomock.Any(), gomock.Any()).Return(errSomeError)

	w := &actions.HookOutputWriter{
		RunID:            runID,
		HookRunID:        hookRunID,
		StorageNamespace: "storageNamespace",
		ActionName:       "actionName",
		HookID:           "hookID",
		Writer:           writer,
	}
	contentReader := strings.NewReader("content")
	err := w.OutputWrite(ctx, contentReader, 10)
	if !errors.Is(err, errSomeError) {
		t.Fatalf("OutputWrite() err=%v expected=%v", err, errSomeError)
	}
}
