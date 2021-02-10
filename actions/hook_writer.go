package actions

import (
	"context"
	"io"
	"path"
)

type HookWriter struct {
	RunID      string
	ActionName string
	HookID     string
	Writer     OutputWriter
}

func (h *HookWriter) OutputWrite(ctx context.Context, name string, reader io.Reader) error {
	outputPath := FormatHookOutputPath(h.RunID, h.ActionName, h.HookID, name)
	return h.Writer.OutputWrite(ctx, outputPath, reader)
}

func FormatHookOutputPath(runID, actionName, hookID, name string) string {
	return path.Join(runID, actionName, hookID+"_"+name)
}
