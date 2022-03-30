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

// HookRecord is an aggregation of all necessary fields for all event types
// Required fields for all event types:
// RunID
// EventType
// RepositoryID
// StorageNamespace
// SourceRef - The reference from which the actions files are read from
// Event specific fields:
// BranchID - Relevant for all event types except tags. For merge events this will be the ID of the destination branch
// Commit - Relevant only for commit and merge events. In both it will contain the new commit data created from the operation
// CommitID - Not relevant in delete branch. In commit and merge will not exist in pre-action. In post actions will contain the new commit ID
// PreRunID - Exists only in post actions. Contains the ID of the pre-action associated with this post-action
// TagID - Exists only in tag actions.
type HookRecord struct {
	RunID            string
	EventType        EventType
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	SourceRef        Ref
	BranchID         BranchID
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
