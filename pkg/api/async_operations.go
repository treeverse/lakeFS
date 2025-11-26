package api

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/validator"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		metadata catalog.Metadata,
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
	) (*catalog.CommitAsyncStatus, error)

	// SubmitMerge starts an async merge operation and returns a task ID
	// Parameters match the MergeAsync function signature
	SubmitMerge(
		ctx context.Context,
		repositoryID string,
		destinationBranch string,
		sourceRef string,
		committer string,
		message string,
		metadata map[string]string,
		strategy string,
		opts ...graveler.SetOptionsFunc,
	) (taskID string, err error)

	// GetMergeStatus returns the status of an async merge operation
	GetMergeStatus(
		ctx context.Context,
		repository string,
		taskID string,
	) (*catalog.MergeAsyncStatus, error)
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

// EnterpriseAsyncOperationsHandler is the enterprise implementation that performs async operations
type EnterpriseAsyncOperationsHandler struct {
	catalog *catalog.Catalog
}

// NewEnterpriseAsyncOperationsHandler creates a new enterprise async operations handler
func NewEnterpriseAsyncOperationsHandler(c *catalog.Catalog) *EnterpriseAsyncOperationsHandler {
	return &EnterpriseAsyncOperationsHandler{
		catalog: c,
	}
}

// SubmitCommit starts an async commit operation and returns a task ID
func (h *EnterpriseAsyncOperationsHandler) SubmitCommit(
	ctx context.Context,
	repositoryID string,
	branch string,
	message string,
	committer string,
	metadata catalog.Metadata,
	date *int64,
	sourceMetarange *string,
	allowEmpty bool,
	opts ...graveler.SetOptionsFunc,
) (string, error) {
	branchID := graveler.BranchID(branch)
	repoID := graveler.RepositoryID(repositoryID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repoID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return "", err
	}

	repository, err := h.catalog.Store.GetRepository(ctx, repoID)
	if err != nil {
		return "", err
	}
	if repository.ReadOnly {
		return "", graveler.ErrReadOnlyRepository
	}

	taskStatus := &catalog.CommitAsyncStatus{}
	taskSteps := []catalog.TaskStep{
		{
			Name: "commit async on " + repositoryID + "/" + branch,
			Func: func(ctx context.Context) error {
				p := graveler.CommitParams{
					Committer:  committer,
					Message:    message,
					Date:       date,
					Metadata:   map[string]string(metadata),
					AllowEmpty: allowEmpty,
				}
				if sourceMetarange != nil {
					x := graveler.MetaRangeID(*sourceMetarange)
					p.SourceMetaRange = &x
				}
				commitID, err := h.catalog.Store.Commit(ctx, repository, branchID, p, opts...)
				if err != nil {
					return err
				}
				commitInfo := &catalog.CommitAsyncInfo{
					Id:        commitID.String(),
					Committer: committer,
					Message:   message,
					Metadata:  map[string]string(metadata),
				}
				// in order to return commit log we need the commit creation time and parents
				commit, err := h.catalog.Store.GetCommit(ctx, repository, commitID)
				if err != nil {
					taskStatus.Info = commitInfo
					return graveler.ErrCommitNotFound
				}
				for _, parent := range commit.Parents {
					commitInfo.Parents = append(commitInfo.Parents, parent.String())
				}
				commitInfo.CreationDate = timestamppb.New(commit.CreationDate)
				commitInfo.MetaRangeId = string(commit.MetaRangeID)
				commitInfo.Version = int32(commit.Version)
				commitInfo.Generation = int64(commit.Generation)
				taskStatus.Info = commitInfo
				return nil
			},
		},
	}

	taskID := catalog.NewTaskID(catalog.CommitAsyncPrefix)
	err = h.catalog.RunBackgroundTaskSteps(repository, taskID, taskSteps, taskStatus)
	if err != nil {
		return "", err
	}
	return taskID, nil
}

// GetCommitStatus returns the status of an async commit operation
func (h *EnterpriseAsyncOperationsHandler) GetCommitStatus(
	ctx context.Context,
	repository string,
	taskID string,
) (*catalog.CommitAsyncStatus, error) {
	var status catalog.CommitAsyncStatus
	err := h.catalog.GetTaskStatusGeneric(ctx, repository, taskID, catalog.CommitAsyncPrefix, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

// SubmitMerge starts an async merge operation and returns a task ID
func (h *EnterpriseAsyncOperationsHandler) SubmitMerge(
	ctx context.Context,
	repositoryID string,
	destinationBranch string,
	sourceRef string,
	committer string,
	message string,
	metadata map[string]string,
	strategy string,
	opts ...graveler.SetOptionsFunc,
) (string, error) {
	destination := graveler.BranchID(destinationBranch)
	source := graveler.Ref(sourceRef)
	repoID := graveler.RepositoryID(repositoryID)
	meta := graveler.Metadata(metadata)
	commitParams := graveler.CommitParams{
		Committer: committer,
		Message:   message,
		Metadata:  meta,
	}
	if commitParams.Message == "" {
		commitParams.Message = fmt.Sprintf("Merge '%s' into '%s'", source, destination)
	}
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repoID, Fn: graveler.ValidateRepositoryID},
		{Name: "destination", Value: destination, Fn: graveler.ValidateBranchID},
		{Name: "source", Value: source, Fn: graveler.ValidateRef},
		{Name: "committer", Value: commitParams.Committer, Fn: validator.ValidateRequiredString},
		{Name: "message", Value: commitParams.Message, Fn: validator.ValidateRequiredString},
		{Name: "strategy", Value: strategy, Fn: graveler.ValidateRequiredStrategy},
	}); err != nil {
		return "", err
	}

	repository, err := h.catalog.Store.GetRepository(ctx, repoID)
	if err != nil {
		return "", err
	}
	if repository.ReadOnly {
		return "", graveler.ErrReadOnlyRepository
	}

	taskStatus := &catalog.MergeAsyncStatus{}
	taskSteps := []catalog.TaskStep{
		{
			Name: "merge async " + source.String() + " into " + destination.String() + " on " + repository.RepositoryID.String(),
			Func: func(ctx context.Context) error {
				// disabling batching for this flow. See #3935 for more details
				ctx = context.WithValue(ctx, batch.SkipBatchContextKey, struct{}{})

				commitID, err := h.catalog.Store.Merge(ctx, repository, destination, source, commitParams, strategy, opts...)
				if err != nil {
					return err
				}
				taskStatus.Info = &catalog.MergeAsyncInfo{
					Reference: commitID.String(),
				}
				return nil
			},
		},
	}

	taskID := catalog.NewTaskID(catalog.MergeAsyncPrefix)
	err = h.catalog.RunBackgroundTaskSteps(repository, taskID, taskSteps, taskStatus)
	if err != nil {
		return "", err
	}
	return taskID, nil
}

// GetMergeStatus returns the status of an async merge operation
func (h *EnterpriseAsyncOperationsHandler) GetMergeStatus(
	ctx context.Context,
	repository string,
	taskID string,
) (*catalog.MergeAsyncStatus, error) {
	var status catalog.MergeAsyncStatus
	err := h.catalog.GetTaskStatusGeneric(ctx, repository, taskID, catalog.MergeAsyncPrefix, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
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
	metadata catalog.Metadata,
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
) (*catalog.CommitAsyncStatus, error) {
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
	metadata map[string]string,
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
) (*catalog.MergeAsyncStatus, error) {
	return nil, auth.ErrNotImplemented
}
