package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type AsyncOperationsHandler interface {
	SubmitCommit(
		ctx context.Context,
		repository string,
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
		repository string,
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
		repository string,
		taskID string,
	) (*MergeAsyncStatus, error)
}

type OSSAsyncOperationsHandler struct{}

func NewOSSAsyncOperationsHandler() *OSSAsyncOperationsHandler {
	return &OSSAsyncOperationsHandler{}
}

func (h *OSSAsyncOperationsHandler) SubmitCommit(
	ctx context.Context,
	repository string,
	branch string,
	message string,
	committer string,
	metadata Metadata,
	date *int64,
	sourceMetarange *string,
	allowEmpty bool,
	opts ...graveler.SetOptionsFunc,
) (string, error) {
	return "", auth.ErrNotImplemented
}

func (h *OSSAsyncOperationsHandler) GetCommitStatus(
	ctx context.Context,
	repository string,
	taskID string,
) (*CommitAsyncStatus, error) {
	return nil, auth.ErrNotImplemented
}

func (h *OSSAsyncOperationsHandler) SubmitMergeIntoBranch(
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
	return "", auth.ErrNotImplemented
}

func (h *OSSAsyncOperationsHandler) GetMergeIntoBranchStatus(
	ctx context.Context,
	repository string,
	taskID string,
) (*MergeAsyncStatus, error) {
	return nil, auth.ErrNotImplemented
}
