package actions

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/db"

	"github.com/treeverse/lakefs/pkg/kv"

	"github.com/treeverse/lakefs/pkg/graveler"
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
	store kv.StoreMessage
}

func NewActionsKVStore(store kv.StoreMessage) Store {
	return &KVStore{store: store}
}

func (kvs *KVStore) GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error) {
	runKey := RunPath(repositoryID, runID)
	m := RunResultData{}
	_, err := kvs.store.GetMsg(ctx, PartitionKey, runKey, &m)
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
	_, err := kvs.store.GetMsg(ctx, PartitionKey, runKey, &m)
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
	_, err := kvs.store.GetMsg(ctx, PartitionKey, runKey, &run)
	if err != nil {
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
		taskKey := kv.FormatPath(TasksPath(repositoryID.String(), manifest.Run.RunID), hookRun.HookRunID)
		err := kvs.store.SetMsgIf(ctx, PartitionKey, taskKey, protoFromTaskResult(&hookRun), nil)
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
		err := kvs.store.SetMsg(ctx, PartitionKey, bk, &kv.SecondaryIndex{PrimaryKey: []byte(runKey)})
		if err != nil {
			return fmt.Errorf("save secondary index by branch (key %s): %w", bk, err)
		}
	}

	// Save secondary index by CommitID
	if run.CommitId != "" {
		ck := RunByCommitPath(repoID, run.CommitId, run.RunId)
		err := kvs.store.SetMsg(ctx, PartitionKey, ck, &kv.SecondaryIndex{PrimaryKey: []byte(runKey)})
		if err != nil {
			return fmt.Errorf("save secondary index by commit (key %s): %w", ck, err)
		}
	}

	err := kvs.store.SetMsg(ctx, PartitionKey, runKey, run)
	if err != nil {
		return fmt.Errorf("save run result (runKey %s): %w", runKey, err)
	}

	return nil
}

type DBStore struct {
	db db.Database
}

func NewActionsDBStore(db db.Database) Store {
	return &DBStore{db: db}
}

func (dbs *DBStore) GetRunResult(ctx context.Context, repositoryID, runID string) (*RunResult, error) {
	res, err := dbs.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return dbs.getRunResultTx(tx, repositoryID, runID)
	}, db.ReadOnly())
	if errors.Is(err, db.ErrNotFound) {
		return nil, fmt.Errorf("run id %s: %w", runID, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return res.(*RunResult), nil
}

func (dbs *DBStore) getRunResultTx(tx db.Tx, repositoryID string, runID string) (*RunResult, error) {
	result := &RunResult{
		RunID: runID,
	}
	err := tx.Get(result, `SELECT event_type, branch_id, source_ref, start_time, end_time, passed, commit_id
			FROM actions_runs
			WHERE repository_id=$1 AND run_id=$2`,
		repositoryID, runID)
	if err != nil {
		return nil, fmt.Errorf("get run result: %w", err)
	}
	return result, nil
}

func (dbs *DBStore) GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error) {
	res, err := dbs.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		result := &TaskResult{
			RunID:     runID,
			HookRunID: hookRunID,
		}
		err := tx.Get(result, `SELECT hook_id, action_name, start_time, end_time, passed
			FROM actions_run_hooks 
			WHERE repository_id=$1 AND run_id=$2 AND hook_run_id=$3`,
			repositoryID, runID, hookRunID)
		if err != nil {
			return nil, fmt.Errorf("get task result: %w", err)
		}
		return result, nil
	}, db.ReadOnly())
	if errors.Is(err, db.ErrNotFound) {
		return nil, fmt.Errorf("hook run id %s/%s: %w", runID, hookRunID, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return res.(*TaskResult), nil
}

func (dbs *DBStore) ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (RunResultIterator, error) {
	return NewDBRunResultIterator(ctx, dbs.db, defaultFetchSize, repositoryID, branchID, commitID, after), nil
}

func (dbs *DBStore) ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error) {
	return NewDBTaskResultIterator(ctx, dbs.db, defaultFetchSize, repositoryID, runID, after), nil
}

// UpdateCommitID assume record is a post event, we use the PreRunID to update the commit_id and save the run manifest again
func (dbs *DBStore) UpdateCommitID(ctx context.Context, repositoryID string, runID string, commitID string) (*RunManifest, error) {
	if runID == "" {
		return nil, fmt.Errorf("run id: %w", ErrNotFound)
	}

	// update database and re-read the run manifest
	var manifest *RunManifest
	_, err := dbs.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// update commit id
		res, err := tx.Exec(`UPDATE actions_runs SET commit_id=$3 WHERE repository_id=$1 AND run_id=$2`,
			repositoryID, runID, commitID)
		if err != nil {
			return nil, fmt.Errorf("update run commit_id: %w", err)
		}
		// return if nothing was updated
		if res.RowsAffected() == 0 {
			return nil, nil
		}

		// read run information
		runResult, err := dbs.getRunResultTx(tx, repositoryID, runID)
		if err != nil {
			return nil, err
		}
		manifest = &RunManifest{Run: *runResult}

		// read tasks information
		err = tx.Select(&manifest.HooksRun, `SELECT run_id, hook_run_id, hook_id, action_name, start_time, end_time, passed
			FROM actions_run_hooks 
			WHERE repository_id=$1 AND run_id=$2`,
			repositoryID, runID)
		if err != nil {
			return nil, fmt.Errorf("get tasks result: %w", err)
		}
		return nil, nil
	})
	if errors.Is(err, db.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil || manifest == nil {
		return nil, err
	}

	return manifest, nil
}

func (dbs *DBStore) saveRunManifest(ctx context.Context, repositoryID graveler.RepositoryID, manifest RunManifest) error {
	_, err := dbs.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// insert run information
		run := manifest.Run
		_, err := tx.Exec(`INSERT INTO actions_runs(repository_id, run_id, event_type, start_time, end_time, branch_id, source_ref, commit_id, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			repositoryID, run.RunID, run.EventType, run.StartTime, run.EndTime, run.BranchID, run.SourceRef, run.CommitID, run.Passed)
		if err != nil {
			return nil, fmt.Errorf("insert run information %s: %w", run.RunID, err)
		}

		// insert each task information
		for _, hookRun := range manifest.HooksRun {
			_, err = tx.Exec(`INSERT INTO actions_run_hooks(repository_id, run_id, hook_run_id, action_name, hook_id, start_time, end_time, passed)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				repositoryID, hookRun.RunID, hookRun.HookRunID, hookRun.ActionName, hookRun.HookID, hookRun.StartTime, hookRun.EndTime, hookRun.Passed)
			if err != nil {
				return nil, fmt.Errorf("insert run hook information %s/%s: %w", hookRun.RunID, hookRun.HookRunID, err)
			}
		}
		return nil, nil
	})
	return err
}
