package actions

import (
	"context"
	"io"
	"path"
)

type OutputWriter interface {
	OutputWrite(ctx context.Context, storageNamespace, path string, reader io.Reader, size int64) error
}

// Simple wrapper for OutputWriter with pre-determined name
type HookOutputWriter struct {
	path             string
	storageNamespace string
	wr               OutputWriter
}

func (w *HookOutputWriter) write(ctx context.Context, reader io.Reader, size int64) error {
	return w.wr.OutputWrite(ctx, w.storageNamespace, w.path, reader, size)
}

func FormatHookOutputPath(runID, actionName, hookID string) string {
	return path.Join(runID, actionName, hookID)
}
