package graveler

import (
	context "context"

	"github.com/google/uuid"
)

type HooksHandler interface {
	PreCommitHook(ctx context.Context, eventID uuid.UUID, repositoryRecord RepositoryRecord, branch BranchID, commit Commit) error
	PostCommitHook(ctx context.Context, eventID uuid.UUID, repositoryRecord RepositoryRecord, branch BranchID, commitRecord CommitRecord) error
	PreMergeHook(ctx context.Context, eventID uuid.UUID, repositoryRecord RepositoryRecord, destination BranchID, source Ref, commit Commit) error
	PostMergeHook(ctx context.Context, eventID uuid.UUID, repositoryRecord RepositoryRecord, destination BranchID, source Ref, commitRecord CommitRecord) error
}

type HooksNoOp struct{}

func (h *HooksNoOp) PreCommitHook(context.Context, uuid.UUID, RepositoryRecord, BranchID, Commit) error {
	return nil
}

func (h *HooksNoOp) PostCommitHook(context.Context, uuid.UUID, RepositoryRecord, BranchID, CommitRecord) error {
	return nil
}

func (h *HooksNoOp) PreMergeHook(context.Context, uuid.UUID, RepositoryRecord, BranchID, Ref, Commit) error {
	return nil
}

func (h *HooksNoOp) PostMergeHook(context.Context, uuid.UUID, RepositoryRecord, BranchID, Ref, CommitRecord) error {
	return nil
}
