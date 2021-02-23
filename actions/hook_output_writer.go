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
	runManifestFilename = "run.manifest"
)

func (h *HookOutputWriter) OutputWrite(ctx context.Context, reader io.Reader, size int64) error {
	name := FormatHookOutputPath(h.RunID, h.HookRunID)
	return h.Writer.OutputWrite(ctx, h.StorageNamespace, name, reader, size)
}

func FormatHookOutputPath(runID, hookRunID string) string {
	return path.Join(outputLocation, runID, hookRunID+hookOutputExtension)
}

func FormatRunManifestOutputPath(runID string) string {
	return path.Join(outputLocation, runID, runManifestFilename)
}
