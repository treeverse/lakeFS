package actions

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
)

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
	return NewDBRunResultIterator(ctx, dbs.db, defaultFetchSize, repositoryID, branchID, commitID, after)
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
