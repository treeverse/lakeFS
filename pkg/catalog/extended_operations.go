package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type ExtendedOperations interface {
	SubmitCommit(
		ctx context.Context,
		repositoryID string,
		branch string,
		message string,
		committer string,
		metadata Metadata,
		date *int64,
		sourceMetarange *string,
		allowEmpty bool,
		opts ...graveler.SetOptionsFunc,
	) (taskID string, err error)

	GetCommitStatus(
		ctx context.Context,
		repositoryID string,
		taskID string,
	) (*CommitAsyncStatusData, error)

	SubmitMergeIntoBranch(
		ctx context.Context,
		repositoryID string,
		destinationBranch string,
		sourceRef string,
		committer string,
		message string,
		metadata Metadata,
		strategy string,
		opts ...graveler.SetOptionsFunc,
	) (taskID string, err error)

	GetMergeIntoBranchStatus(
		ctx context.Context,
		repositoryID string,
		taskID string,
	) (*MergeAsyncStatusData, error)
}

type NoopExtendedOperations struct{}

func NewNoopExtendedOperations() *NoopExtendedOperations {
	return &NoopExtendedOperations{}
}

func (h *NoopExtendedOperations) SubmitCommit(
	ctx context.Context,
	repositoryID string,
	branch string,
	message string,
	committer string,
	metadata Metadata,
	date *int64,
	sourceMetarange *string,
	allowEmpty bool,
	opts ...graveler.SetOptionsFunc,
) (string, error) {
	return "", ErrNotImplemented
}

func (h *NoopExtendedOperations) GetCommitStatus(
	ctx context.Context,
	repositoryID string,
	taskID string,
) (*CommitAsyncStatusData, error) {
	return nil, ErrNotImplemented
}

func (h *NoopExtendedOperations) SubmitMergeIntoBranch(
	ctx context.Context,
	repositoryID string,
	destinationBranch string,
	sourceRef string,
	committer string,
	message string,
	metadata Metadata,
	strategy string,
	opts ...graveler.SetOptionsFunc,
) (string, error) {
	return "", ErrNotImplemented
}

func (h *NoopExtendedOperations) GetMergeIntoBranchStatus(
	ctx context.Context,
	repositoryID string,
	taskID string,
) (*MergeAsyncStatusData, error) {
	return nil, ErrNotImplemented
}
