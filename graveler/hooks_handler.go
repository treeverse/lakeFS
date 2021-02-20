package graveler

import (
	"context"
	"time"

	nanoid "github.com/matoous/go-nanoid"
)

type EventType string

const (
	EventTypePreCommit  EventType = "pre-commit"
	EventTypePostCommit EventType = "post-commit"
	EventTypePreMerge   EventType = "pre-merge"
	EventTypePostMerge  EventType = "post-merge"
)

type HookRecord struct {
	RunID            string
	EventType        EventType
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	BranchID         BranchID
	SourceRef        Ref
	Commit           Commit
	CommitID         CommitID
	PreRunID         string
}

type HooksHandler interface {
	PreCommitHook(ctx context.Context, record HookRecord) error
	PostCommitHook(ctx context.Context, record HookRecord) error
	PreMergeHook(ctx context.Context, record HookRecord) error
	PostMergeHook(ctx context.Context, record HookRecord) error
}

type HooksNoOp struct{}

func (h *HooksNoOp) PreCommitHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostCommitHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PreMergeHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostMergeHook(context.Context, HookRecord) error {
	return nil
}

func NewRunID() string {
	const nanoLen = 8
	id := nanoid.MustID(nanoLen)
	tm := time.Now().UTC().Format("20060102150405")
	return tm + id
}
