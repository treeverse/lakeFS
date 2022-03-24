package actions

//go:generate mockgen -package=mock -destination=mock/mock_actions.go github.com/treeverse/lakefs/pkg/actions Source,OutputWriter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

type Service struct {
	DB       db.Database
	Source   Source
	Writer   OutputWriter
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stats    stats.Collector
	runHooks bool
}

type Task struct {
	RunID     string
	HookRunID string
	Action    *Action
	HookID    string
	Hook      Hook
	Err       error
	StartTime time.Time
	EndTime   time.Time
}

type RunResult struct {
	RunID     string    `db:"run_id" json:"run_id"`
	BranchID  string    `db:"branch_id" json:"branch_id"`
	SourceRef string    `db:"source_ref" json:"source_ref"`
	EventType string    `db:"event_type" json:"event_type"`
	StartTime time.Time `db:"start_time" json:"start_time"`
	EndTime   time.Time `db:"end_time" json:"end_time"`
	Passed    bool      `db:"passed" json:"passed"`
	CommitID  string    `db:"commit_id" json:"commit_id,omitempty"`
}

type TaskResult struct {
	RunID      string    `db:"run_id" json:"run_id"`
	HookRunID  string    `db:"hook_run_id" json:"hook_run_id"`
	HookID     string    `db:"hook_id" json:"hook_id"`
	ActionName string    `db:"action_name" json:"action_name"`
	StartTime  time.Time `db:"start_time" json:"start_time"`
	EndTime    time.Time `db:"end_time" json:"end_time"`
	Passed     bool      `db:"passed" json:"passed"`
}

type RunManifest struct {
	Run      RunResult    `json:"run"`
	HooksRun []TaskResult `json:"hooks,omitempty"`
}

func (r *TaskResult) LogPath() string {
	return FormatHookOutputPath(r.RunID, r.HookRunID)
}

type RunResultIterator interface {
	Next() bool
	Value() *RunResult
	Err() error
	Close()
}

type TaskResultIterator interface {
	Next() bool
	Value() *TaskResult
	Err() error
	Close()
}

const defaultFetchSize = 1024

var ErrNotFound = errors.New("not found")

func NewService(ctx context.Context, db db.Database, source Source, writer OutputWriter, stats stats.Collector, runHooks bool) *Service {
	ctx, cancel := context.WithCancel(ctx)
	return &Service{
		DB:       db,
		Source:   source,
		Writer:   writer,
		ctx:      ctx,
		cancel:   cancel,
		wg:       sync.WaitGroup{},
		stats:    stats,
		runHooks: runHooks,
	}
}

func (s *Service) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *Service) asyncRun(record graveler.HookRecord) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// passing the global context for cancelling all runs when lakeFS shuts down
		if err := s.Run(s.ctx, record); err != nil {
			logging.Default().WithError(err).WithField("record", record).
				Info("Async run of hook failed")
		}
	}()
}

// Run load and run actions based on the event information
func (s *Service) Run(ctx context.Context, record graveler.HookRecord) error {
	if !s.runHooks {
		logging.Default().WithField("record", record).Info("Hooks are disabled, skipping hooks execution")
		return nil
	}

	// load relevant actions
	spec := MatchSpec{
		EventType: record.EventType,
		BranchID:  record.BranchID,
	}
	logging.Default().WithField("record", record).WithField("spec", spec).Info("Filtering actions")
	actions, err := s.loadMatchedActions(ctx, record, spec)
	if err != nil || len(actions) == 0 {
		return err
	}

	// allocate and run hooks
	tasks, err := s.allocateTasks(record.RunID, actions)
	if err != nil {
		return err
	}

	runErr := s.runTasks(ctx, record, tasks)

	// keep results before returning an error (if any)
	err = s.saveRunInformation(ctx, record, tasks)
	if err != nil {
		return err
	}

	return runErr
}

func (s *Service) loadMatchedActions(ctx context.Context, record graveler.HookRecord, spec MatchSpec) ([]*Action, error) {
	actions, err := LoadActions(ctx, s.Source, record)
	if err != nil {
		return nil, err
	}
	return MatchedActions(actions, spec)
}

func (s *Service) allocateTasks(runID string, actions []*Action) ([][]*Task, error) {
	var tasks [][]*Task
	for actionIdx, action := range actions {
		var actionTasks []*Task
		for hookIdx, hook := range action.Hooks {
			h, err := NewHook(hook, action)
			if err != nil {
				return nil, err
			}
			task := &Task{
				RunID:     runID,
				HookRunID: NewHookRunID(actionIdx, hookIdx),
				Action:    action,
				HookID:    hook.ID,
				Hook:      h,
			}
			// append new task or chain to the last one based on the current action
			actionTasks = append(actionTasks, task)
		}
		if len(actionTasks) > 0 {
			tasks = append(tasks, actionTasks)
		}
	}
	return tasks, nil
}

func (s *Service) runTasks(ctx context.Context, record graveler.HookRecord, tasks [][]*Task) error {
	var g multierror.Group
	for _, actionTasks := range tasks {
		actionTasks := actionTasks // pin
		g.Go(func() error {
			for _, task := range actionTasks {
				hookOutputWriter := &HookOutputWriter{
					Writer:           s.Writer,
					StorageNamespace: record.StorageNamespace.String(),
					RunID:            task.RunID,
					HookRunID:        task.HookRunID,
					ActionName:       task.Action.Name,
					HookID:           task.HookID,
				}
				task.StartTime = time.Now().UTC()
				task.Err = task.Hook.Run(ctx, record, hookOutputWriter)
				task.EndTime = time.Now().UTC()

				s.stats.CollectEvent("actions_service", string(record.EventType))

				if task.Err != nil {
					// wrap error with more information and return
					task.Err = fmt.Errorf("hook run id '%s' failed on action '%s' hook '%s': %w",
						task.HookRunID, task.Action.Name, task.HookID, task.Err)
					return task.Err
				}
			}
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
}

func (s *Service) saveRunInformation(ctx context.Context, record graveler.HookRecord, tasks [][]*Task) error {
	if len(tasks) == 0 {
		return nil
	}

	manifest := buildRunManifestFromTasks(record, tasks)

	err := s.saveRunManifestDB(ctx, record.RepositoryID, manifest)
	if err != nil {
		return fmt.Errorf("insert run information: %w", err)
	}

	return s.saveRunManifestObjectStore(ctx, manifest, record.StorageNamespace.String(), record.RunID)
}

func (s *Service) saveRunManifestObjectStore(ctx context.Context, manifest RunManifest, storageNamespace string, runID string) error {
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal run manifest: %w", err)
	}
	runManifestPath := FormatRunManifestOutputPath(runID)
	manifestReader := bytes.NewReader(manifestJSON)
	manifestSize := int64(len(manifestJSON))
	return s.Writer.OutputWrite(ctx, storageNamespace, runManifestPath, manifestReader, manifestSize)
}

func (s *Service) saveRunManifestDB(ctx context.Context, repositoryID graveler.RepositoryID, manifest RunManifest) error {
	_, err := s.DB.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func buildRunManifestFromTasks(record graveler.HookRecord, tasks [][]*Task) RunManifest {
	manifest := RunManifest{
		Run: RunResult{
			RunID:     record.RunID,
			BranchID:  record.BranchID.String(),
			SourceRef: record.SourceRef.String(),
			EventType: string(record.EventType),
			Passed:    true,
			CommitID:  record.CommitID.String(),
		},
	}
	for _, actionTasks := range tasks {
		for _, task := range actionTasks {
			// skip scan when task didn't run
			if task.StartTime.IsZero() {
				break
			}
			// record hook run information
			manifest.HooksRun = append(manifest.HooksRun, TaskResult{
				RunID:      task.RunID,
				HookRunID:  task.HookRunID,
				HookID:     task.HookID,
				ActionName: task.Action.Name,
				StartTime:  task.StartTime,
				EndTime:    task.EndTime,
				Passed:     task.Err == nil,
			})
			// keep min run start time
			if manifest.Run.StartTime.IsZero() || task.StartTime.Before(manifest.Run.StartTime) {
				manifest.Run.StartTime = task.StartTime
			}
			// keep max run end time
			if manifest.Run.EndTime.IsZero() || task.EndTime.After(manifest.Run.EndTime) {
				manifest.Run.EndTime = task.EndTime
			}
			// did we failed
			manifest.Run.Passed = manifest.Run.Passed && task.Err == nil
		}
	}

	return manifest
}

// UpdateCommitID assume record is a post event, we use the PreRunID to update the commit_id and save the run manifest again
func (s *Service) UpdateCommitID(ctx context.Context, repositoryID string, storageNamespace string, runID string, commitID string) error {
	if runID == "" {
		return fmt.Errorf("run id: %w", ErrNotFound)
	}

	// update database and re-read the run manifest
	var manifest *RunManifest
	_, err := s.DB.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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
		runResult, err := s.getRunResultTx(tx, repositoryID, runID)
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
		return ErrNotFound
	}
	if err != nil || manifest == nil {
		return err
	}

	// update manifest
	return s.saveRunManifestObjectStore(ctx, *manifest, storageNamespace, runID)
}

func (s *Service) GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error) {
	res, err := s.DB.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return s.getRunResultTx(tx, repositoryID, runID)
	}, db.ReadOnly())
	if errors.Is(err, db.ErrNotFound) {
		return nil, fmt.Errorf("run id %s: %w", runID, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return res.(*RunResult), nil
}

func (s *Service) getRunResultTx(tx db.Tx, repositoryID string, runID string) (*RunResult, error) {
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

func (s *Service) GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error) {
	res, err := s.DB.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func (s *Service) ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (RunResultIterator, error) {
	return NewDBRunResultIterator(ctx, s.DB, defaultFetchSize, repositoryID, branchID, commitID, after), nil
}

func (s *Service) ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error) {
	return NewDBTaskResultIterator(ctx, s.DB, defaultFetchSize, repositoryID, runID, after), nil
}

func (s *Service) PreCommitHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostCommitHook(ctx context.Context, record graveler.HookRecord) error {
	// update pre-commit with commit ID if needed
	err := s.UpdateCommitID(ctx, record.RepositoryID.String(), record.StorageNamespace.String(), record.PreRunID, record.CommitID.String())
	if err != nil {
		return err
	}

	s.asyncRun(record)
	return nil
}

func (s *Service) PreMergeHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostMergeHook(ctx context.Context, record graveler.HookRecord) error {
	// update pre-merge with commit ID if needed
	err := s.UpdateCommitID(ctx, record.RepositoryID.String(), record.StorageNamespace.String(), record.PreRunID, record.CommitID.String())
	if err != nil {
		return err
	}

	s.asyncRun(record)
	return nil
}

func (s *Service) PreCreateTagHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostCreateTagHook(_ context.Context, record graveler.HookRecord) error {
	s.asyncRun(record)
	return nil
}

func (s *Service) PreDeleteTagHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostDeleteTagHook(_ context.Context, record graveler.HookRecord) error {
	s.asyncRun(record)
	return nil
}

func (s *Service) PreCreateBranchHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostCreateBranchHook(_ context.Context, record graveler.HookRecord) error {
	s.asyncRun(record)
	return nil
}

func (s *Service) PreDeleteBranchHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostDeleteBranchHook(_ context.Context, record graveler.HookRecord) error {
	s.asyncRun(record)
	return nil
}

func NewHookRunID(actionIdx, hookIdx int) string {
	return fmt.Sprintf("%04d_%04d", actionIdx, hookIdx)
}
