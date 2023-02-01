package actions

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// Store is an abstraction layer for operating with a concrete postgres DB or a
// KV store abstraction.
type Store interface {
	// UpdateCommitID will update an already stored run with the commit results
	UpdateCommitID(ctx context.Context, repositoryID string, runID string, commitID string) (*RunManifest, error)

	// saveRunManifest saves the run and all the hooks information to the underlying store
	saveRunManifest(ctx context.Context, repositoryID graveler.RepositoryID, manifest RunManifest) error

	GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error)
	GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error)
	ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (RunResultIterator, error)
	ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error)
}

type KVStore struct {
	store kv.Store
}

func NewActionsKVStore(store kv.Store) Store {
	return &KVStore{store: store}
}

func (kvs *KVStore) GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error) {
	runKey := RunPath(repositoryID, runID)
	m := RunResultData{}
	_, err := kv.GetMsg(ctx, kvs.store, PartitionKey, runKey, &m)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = fmt.Errorf("%s: %w", err, ErrNotFound) // Wrap error for compatibility with DBService
		}
		return nil, err
	}
	return RunResultFromProto(&m), nil
}

func (kvs *KVStore) GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error) {
	runKey := kv.FormatPath(TasksPath(repositoryID, runID), hookRunID)
	m := TaskResultData{}
	_, err := kv.GetMsg(ctx, kvs.store, PartitionKey, []byte(runKey), &m)
	if err != nil {
		return nil, err
	}
	return taskResultFromProto(&m), nil
}

func (kvs *KVStore) ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (RunResultIterator, error) {
	return NewKVRunResultIterator(ctx, kvs.store, repositoryID, branchID, commitID, after)
}

func (kvs *KVStore) ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error) {
	return NewKVTaskResultIterator(ctx, kvs.store, repositoryID, runID, after)
}

// UpdateCommitID assume record is a post event, we use the PreRunID to update the commit_id and save the run manifest again
func (kvs *KVStore) UpdateCommitID(ctx context.Context, repositoryID string, runID string, commitID string) (*RunManifest, error) {
	if runID == "" {
		return nil, fmt.Errorf("run id: %w", ErrNotFound)
	}

	runKey := RunPath(repositoryID, runID)
	run := RunResultData{}
	_, err := kv.GetMsg(ctx, kvs.store, PartitionKey, runKey, &run)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) { // no pre action run
			return nil, nil
		}
		return nil, fmt.Errorf("run id %s: %w", runID, err)
	}
	if run.CommitId == commitID { // return if no update is required
		return nil, nil
	}

	// update database and re-read the run manifest
	// update database and re-read the run manifest
	run.CommitId = commitID
	err = kvs.storeRun(ctx, &run, repositoryID)
	if err != nil {
		return nil, fmt.Errorf("update run commit_id: %w", err)
	}

	manifest := &RunManifest{Run: *RunResultFromProto(&run)}

	it, err := NewKVTaskResultIterator(ctx, kvs.store, repositoryID, runID, "")
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var tasks []TaskResult
	for it.Next() {
		res := it.Value()
		if res == nil {
			return nil, ErrNilValue
		}
		tasks = append(tasks, *res)
	}
	if err = it.Err(); err != nil {
		return nil, err
	}
	manifest.HooksRun = tasks

	// update manifest
	return manifest, nil
}

func (kvs *KVStore) saveRunManifest(ctx context.Context, repositoryID graveler.RepositoryID, manifest RunManifest) error {
	// insert each task information
	for i := range manifest.HooksRun {
		hookRun := manifest.HooksRun[i]
		taskKey := []byte(kv.FormatPath(TasksPath(repositoryID.String(), manifest.Run.RunID), hookRun.HookRunID))
		err := kv.SetMsgIf(ctx, kvs.store, PartitionKey, taskKey, protoFromTaskResult(&hookRun), nil)
		if err != nil {
			return fmt.Errorf("save task result (runID: %s taskKey %s): %w", manifest.Run.RunID, taskKey, err)
		}
	}

	// insert run information
	return kvs.storeRun(ctx, protoFromRunResult(&manifest.Run), repositoryID.String())
}

func (kvs *KVStore) storeRun(ctx context.Context, run *RunResultData, repoID string) error {
	runKey := RunPath(repoID, run.RunId)
	// Save secondary index by BranchID
	if run.BranchId != "" {
		bk := RunByBranchPath(repoID, run.BranchId, run.RunId)
		err := kv.SetMsg(ctx, kvs.store, PartitionKey, bk, &kv.SecondaryIndex{PrimaryKey: runKey})
		if err != nil {
			return fmt.Errorf("save secondary index by branch (key %s): %w", bk, err)
		}
	}

	// Save secondary index by CommitID
	if run.CommitId != "" {
		ck := RunByCommitPath(repoID, run.CommitId, run.RunId)
		err := kv.SetMsg(ctx, kvs.store, PartitionKey, ck, &kv.SecondaryIndex{PrimaryKey: runKey})
		if err != nil {
			return fmt.Errorf("save secondary index by commit (key %s): %w", ck, err)
		}
	}

	err := kv.SetMsg(ctx, kvs.store, PartitionKey, runKey, run)
	if err != nil {
		return fmt.Errorf("save run result (runKey %s): %w", runKey, err)
	}

	return nil
}
