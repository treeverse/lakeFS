package actions

import (
	"context"
	"io"
	"path"
)

type HookOutputWriter struct {
	StorageNamespace string
	RunID            string
	HookRunID        string
	ActionName       string
	HookID           string
	Writer           OutputWriter
}

const (
	hookOutputExtension = ".log"
	outputLocation      = "_lakefs/actions/log"
)

func (h *HookOutputWriter) OutputWrite(ctx context.Context, reader io.Reader, size int64) error {
	name := FormatHookOutputPath(h.RunID, h.ActionName, h.HookID)
	return h.Writer.OutputWrite(ctx, h.StorageNamespace, name, reader, size)
}

func FormatHookOutputPath(runID, actionName, hookID string) string {
	return path.Join(outputLocation, runID, actionName, hookID+hookOutputExtension)
}
