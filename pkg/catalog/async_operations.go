package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// AsyncOperationsHandler manages asynchronous operations for commits and merges
type AsyncOperationsHandler interface {
	// SubmitCommit starts an async commit operation and returns a task ID
	// Parameters match the Commit function signature
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

	// GetCommitStatus returns the status of an async commit operation
	GetCommitStatus(
		ctx context.Context,
		repository string,
		taskID string,
	) (*CommitAsyncStatus, error)

	// SubmitMerge starts an async merge operation and returns a task ID
	// Parameters match the MergeAsync function signature
	SubmitMerge(
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

	// GetMergeStatus returns the status of an async merge operation
	GetMergeStatus(
		ctx context.Context,
		repository string,
		taskID string,
	) (*MergeAsyncStatus, error)
}

// OSSAsyncOperationsHandler is the OSS implementation that returns "not implemented" for all operations
type OSSAsyncOperationsHandler struct{}

// NewOSSAsyncOperationsHandler creates a new OSS async operations handler
func NewOSSAsyncOperationsHandler() *OSSAsyncOperationsHandler {
	return &OSSAsyncOperationsHandler{}
}

// SubmitCommit returns not implemented error for OSS
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

// GetCommitStatus returns not implemented error for OSS
func (h *OSSAsyncOperationsHandler) GetCommitStatus(
	ctx context.Context,
	repository string,
	taskID string,
) (*CommitAsyncStatus, error) {
	return nil, auth.ErrNotImplemented
}

// SubmitMerge returns not implemented error for OSS
func (h *OSSAsyncOperationsHandler) SubmitMerge(
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

// GetMergeStatus returns not implemented error for OSS
func (h *OSSAsyncOperationsHandler) GetMergeStatus(
	ctx context.Context,
	repository string,
	taskID string,
) (*MergeAsyncStatus, error) {
	return nil, auth.ErrNotImplemented
}
