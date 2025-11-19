package api

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
)

// AsyncOperationsHandler manages asynchronous operations for commits and merges
type AsyncOperationsHandler interface {
	// SubmitCommit starts an async commit operation and returns a task ID
	// Parameters match the CommitCreation schema from swagger.yml
	// The committer will be extracted from the user context
	SubmitCommit(
		ctx context.Context,
		repository string,
		branch string,
		message string,
		metadata map[string]string,
		date *int64,
		sourceMetarange *string,
		allowEmpty bool,
		force bool,
	) (taskID string, err error)

	// GetCommitStatus returns the status of an async commit operation
	GetCommitStatus(
		ctx context.Context,
		repository string,
		branch string,
		taskID string,
	) (*AsyncCommitStatus, error)

	// SubmitMerge starts an async merge operation and returns a task ID
	// Parameters match the Merge schema from swagger.yml
	// The committer will be extracted from the user context
	SubmitMerge(
		ctx context.Context,
		repository string,
		destinationBranch string,
		sourceRef string,
		message string,
		metadata map[string]string,
		strategy string,
		force bool,
		allowEmpty bool,
		squashMerge bool,
	) (taskID string, err error)

	// GetMergeStatus returns the status of an async merge operation
	GetMergeStatus(
		ctx context.Context,
		repository string,
		sourceRef string,
		destinationBranch string,
		taskID string,
	) (*AsyncMergeStatus, error)
}

// AsyncCommitStatus represents the status of an async commit operation
// Maps to the CommitAsyncStatus schema in swagger.yml
type AsyncCommitStatus struct {
	TaskID     string
	Completed  bool
	UpdateTime time.Time
	// Result contains the commit details (id, parents, committer, message, creation_date,
	// meta_range_id, metadata, generation, version) as defined in the Commit schema
	// Present only when Completed is true and there's no error
	Result *catalog.CommitLog
	Error  error // Error is present only when Completed is true and operation failed
}

// AsyncMergeStatus represents the status of an async merge operation
// Maps to the MergeAsyncStatus schema in swagger.yml
type AsyncMergeStatus struct {
	TaskID     string
	Completed  bool
	UpdateTime time.Time
	// Result contains the merge result (reference) as defined in the MergeResult schema
	// Present only when Completed is true and there's no error
	Result *AsyncMergeResult
	Error  error // Error is present only when Completed is true and operation failed
}

// AsyncMergeResult represents the result of a merge operation
// Maps to the MergeResult schema in swagger.yml
type AsyncMergeResult struct {
	Reference string
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
	metadata map[string]string,
	date *int64,
	sourceMetarange *string,
	allowEmpty bool,
	force bool,
) (string, error) {
	return "", auth.ErrNotImplemented
}

// GetCommitStatus returns not implemented error for OSS
func (h *OSSAsyncOperationsHandler) GetCommitStatus(
	ctx context.Context,
	repository string,
	branch string,
	taskID string,
) (*AsyncCommitStatus, error) {
	return nil, auth.ErrNotImplemented
}

// SubmitMerge returns not implemented error for OSS
func (h *OSSAsyncOperationsHandler) SubmitMerge(
	ctx context.Context,
	repository string,
	destinationBranch string,
	sourceRef string,
	message string,
	metadata map[string]string,
	strategy string,
	force bool,
	allowEmpty bool,
	squashMerge bool,
) (string, error) {
	return "", auth.ErrNotImplemented
}

// GetMergeStatus returns not implemented error for OSS
func (h *OSSAsyncOperationsHandler) GetMergeStatus(
	ctx context.Context,
	repository string,
	sourceRef string,
	destinationBranch string,
	taskID string,
) (*AsyncMergeStatus, error) {
	return nil, auth.ErrNotImplemented
}
