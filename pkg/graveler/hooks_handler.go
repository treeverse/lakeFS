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
type HookRecord struct {
	// Required fields for all event types:
	RunID            string
	EventType        EventType
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	// The reference which the actions files are read from
	SourceRef Ref
	// Event specific fields:
	// Relevant for all event types except tags. For merge events this will be the ID of the destination branch
	BranchID BranchID
	// Relevant only for commit and merge events. In both it will contain the new commit data created from the operation
	Commit Commit
	// Not relevant in delete branch. In commit and merge will not exist in pre-action. In post actions will contain the new commit ID
	CommitID CommitID
	// Exists only in post actions. Contains the ID of the pre-action associated with this post-action
	PreRunID string
	// Exists only in tag actions.
	TagID TagID
}

type HooksHandler interface {
	PreCommitHook(ctx context.Context, record HookRecord) error
	PostCommitHook(ctx context.Context, record HookRecord) error
	PreMergeHook(ctx context.Context, record HookRecord) error
	PostMergeHook(ctx context.Context, record HookRecord) error
	PreCreateTagHook(ctx context.Context, record HookRecord) error
	PostCreateTagHook(ctx context.Context, record HookRecord)
	PreDeleteTagHook(ctx context.Context, record HookRecord) error
	PostDeleteTagHook(ctx context.Context, record HookRecord)
	PreCreateBranchHook(ctx context.Context, record HookRecord) error
	PostCreateBranchHook(ctx context.Context, record HookRecord)
	PreDeleteBranchHook(ctx context.Context, record HookRecord) error
	PostDeleteBranchHook(ctx context.Context, record HookRecord)
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

func (h *HooksNoOp) PostCreateTagHook(context.Context, HookRecord) {
}

func (h *HooksNoOp) PreDeleteTagHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostDeleteTagHook(context.Context, HookRecord) {
}

func (h *HooksNoOp) PreCreateBranchHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostCreateBranchHook(context.Context, HookRecord) {
}

func (h *HooksNoOp) PreDeleteBranchHook(context.Context, HookRecord) error {
	return nil
}

func (h *HooksNoOp) PostDeleteBranchHook(context.Context, HookRecord) {
}

func NewRunID() string {
	const nanoLen = 8
	id := nanoid.Must(nanoLen)
	tm := time.Now().UTC().Format("20060102150405")
	return tm + id
}
