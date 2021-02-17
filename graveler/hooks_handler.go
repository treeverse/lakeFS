package graveler

import (
	"context"
)

type PreCommitRecord struct {
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	BranchID         BranchID
	Commit           Commit
}

type PostCommitRecord struct {
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	BranchID         BranchID
	Commit           Commit
	CommitID         CommitID
	PreRunID         string
}

type PreMergeRecord struct {
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	Destination      BranchID
	Source           Ref
	Commit           Commit
}

type PostMergeRecord struct {
	RepositoryID     RepositoryID
	StorageNamespace StorageNamespace
	Destination      BranchID
	Source           Ref
	Commit           Commit
	CommitID         CommitID
	PreRunID         string
}

type HooksHandler interface {
	PreCommitHook(ctx context.Context, record PreCommitRecord) (string, error)
	PostCommitHook(ctx context.Context, record PostCommitRecord) (string, error)
	PreMergeHook(ctx context.Context, record PreMergeRecord) (string, error)
	PostMergeHook(ctx context.Context, record PostMergeRecord) (string, error)
}

type HooksNoOp struct{}

func (h *HooksNoOp) PreCommitHook(context.Context, PreCommitRecord) (string, error) {
	return "", nil
}

func (h *HooksNoOp) PostCommitHook(context.Context, PostCommitRecord) (string, error) {
	return "", nil
}

func (h *HooksNoOp) PreMergeHook(context.Context, PreMergeRecord) (string, error) {
	return "", nil
}

func (h *HooksNoOp) PostMergeHook(context.Context, PostMergeRecord) (string, error) {
	return "", nil
}
