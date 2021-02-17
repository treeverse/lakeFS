package actions

import (
	"context"
	"io"
	"path"
)

type HookOutputWriter struct {
	RunID      string
	ActionName string
	HookID     string
	Writer     OutputWriter
}

func (h *HookOutputWriter) OutputWrite(ctx context.Context, reader io.Reader, size int64) error {
	outputPath := FormatHookOutputPath(h.RunID, h.ActionName, h.HookID)
	return h.Writer.OutputWrite(ctx, outputPath, reader, size)
}

func FormatHookOutputPath(runID, actionName, hookID string) string {
	return path.Join(runID, actionName, hookID)
}
