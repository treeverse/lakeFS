package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type AsyncOperationsHandler interface {
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
	) (*CommitAsyncStatus, error)

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
	) (*MergeAsyncStatus, error)
}

type NoopAsyncOperationsHandler struct{}

func NewNoopAsyncOperationsHandler() *NoopAsyncOperationsHandler {
	return &NoopAsyncOperationsHandler{}
}

func (h *NoopAsyncOperationsHandler) SubmitCommit(
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

func (h *NoopAsyncOperationsHandler) GetCommitStatus(
	ctx context.Context,
	repositoryID string,
	taskID string,
) (*CommitAsyncStatus, error) {
	return nil, ErrNotImplemented
}

func (h *NoopAsyncOperationsHandler) SubmitMergeIntoBranch(
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

func (h *NoopAsyncOperationsHandler) GetMergeIntoBranchStatus(
	ctx context.Context,
	repositoryID string,
	taskID string,
) (*MergeAsyncStatus, error) {
	return nil, ErrNotImplemented
}
