package graveler

import (
	"context"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
)

type EventType string

const (
	EventTypePreCommit        EventType = "pre-commit"
	EventTypePostCommit       EventType = "post-commit"
	EventTypePreMerge         EventType = "pre-merge"
	EventTypePostMerge        EventType = "post-merge"
	EventTypePreCreateTag     EventType = "pre-create-tag"
	EventTypePostCreateTag    EventType = "post-create-tag"
	EventTypePreDeleteTag     EventType = "pre-delete-tag"
	EventTypePostDeleteTag    EventType = "post-delete-tag"
	EventTypePreCreateBranch  EventType = "pre-create-branch"
	EventTypePostCreateBranch EventType = "post-create-branch"
	EventTypePreDeleteBranch  EventType = "pre-delete-branch"
	EventTypePostDeleteBranch EventType = "post-delete-branch"
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
	TagID            TagID
}

type HooksHandler interface {
	PreCommitHook(ctx context.Context, record HookRecord) error
	PostCommitHook(ctx context.Context, record HookRecord) error
	PreMergeHook(ctx context.Context, record HookRecord) error
	PostMergeHook(ctx context.Context, record HookRecord) error
	PreCreateTagHook(ctx context.Context, record HookRecord) error
	PostCreateTagHook(ctx context.Context, record HookRecord) error
	PreDeleteTagHook(ctx context.Context, record HookRecord) error
	PostDeleteTagHook(ctx context.Context, record HookRecord) error
	PreCreateBranchHook(ctx context.Context, record HookRecord) error
	PostCreateBranchHook(ctx context.Context, record HookRecord) error
	PreDeleteBranchHook(ctx context.Context, record HookRecord) error
	PostDeleteBranchHook(ctx context.Context, record HookRecord) error
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

func (h *HooksNoOp) PreCreateTagHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostCreateTagHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PreDeleteTagHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostDeleteTagHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PreCreateBranchHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostCreateBranchHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PreDeleteBranchHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostDeleteBranchHook(context.Context, HookRecord) error {
	return nil
}

func NewRunID() string {
	const nanoLen = 8
	id := nanoid.Must(nanoLen)
	tm := time.Now().UTC().Format("20060102150405")
	return tm + id
}
