package graveler

import (
	"context"
)

type HooksHandler interface {
	PreCommitHook(ctx context.Context, runID string, repositoryRecord RepositoryRecord, branch BranchID, commit Commit) error
	PostCommitHook(ctx context.Context, runID string, repositoryRecord RepositoryRecord, branch BranchID, commitRecord CommitRecord) error
	PreMergeHook(ctx context.Context, runID string, repositoryRecord RepositoryRecord, destination BranchID, source Ref, commit Commit) error
	PostMergeHook(ctx context.Context, runID string, repositoryRecord RepositoryRecord, destination BranchID, source Ref, commitRecord CommitRecord) error
}

type HooksNoOp struct{}

func (h *HooksNoOp) PreCommitHook(context.Context, string, RepositoryRecord, BranchID, Commit) error {
	return nil
}

func (h *HooksNoOp) PostCommitHook(context.Context, string, RepositoryRecord, BranchID, CommitRecord) error {
	return nil
}

func (h *HooksNoOp) PreMergeHook(context.Context, string, RepositoryRecord, BranchID, Ref, Commit) error {
	return nil
}

func (h *HooksNoOp) PostMergeHook(context.Context, string, RepositoryRecord, BranchID, Ref, CommitRecord) error {
	return nil
}
