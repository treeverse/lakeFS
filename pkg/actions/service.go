package actions

//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -package=mock -destination=mock/mock_actions.go github.com/treeverse/lakefs/pkg/actions Source,OutputWriter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	PartitionKey = "actions"
	reposPrefix  = "repos"
	runsPrefix   = "runs"
	tasksPrefix  = "tasks"
	branchPrefix = "branches"
	commitPrefix = "commits"
)

var (
	ErrNotFound = errors.New("not found")
	ErrNilValue = errors.New("nil value")
)

// StoreService is an implementation of actions.Service that saves
// the run data to the blockstore and to the actions.Store (which is a
// fancy name for a DB - kv style or postgres directly)
type StoreService struct {
	Store    Store
	idGen    IDGenerator
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
	CommitID  string    `db:"commit_id" json:"commit_id,omitempty"`
	StartTime time.Time `db:"start_time" json:"start_time"`
	EndTime   time.Time `db:"end_time" json:"end_time"`
	Passed    bool      `db:"passed" json:"passed"`
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

func RunResultFromProto(pb *RunResultData) *RunResult {
	return &RunResult{
		RunID:     pb.RunId,
		BranchID:  pb.BranchId,
		SourceRef: pb.SourceRef,
		EventType: pb.EventType,
		CommitID:  pb.CommitId,
		StartTime: pb.StartTime.AsTime(),
		EndTime:   pb.EndTime.AsTime(),
		Passed:    pb.Passed,
	}
}

func protoFromRunResult(m *RunResult) *RunResultData {
	return &RunResultData{
		RunId:     m.RunID,
		BranchId:  m.BranchID,
		CommitId:  m.CommitID,
		SourceRef: m.SourceRef,
		EventType: m.EventType,
		StartTime: timestamppb.New(m.StartTime),
		EndTime:   timestamppb.New(m.EndTime),
		Passed:    m.Passed,
	}
}

func taskResultFromProto(pb *TaskResultData) *TaskResult {
	return &TaskResult{
		RunID:      pb.RunId,
		HookRunID:  pb.HookRunId,
		HookID:     pb.HookId,
		ActionName: pb.ActionName,
		StartTime:  pb.StartTime.AsTime(),
		EndTime:    pb.EndTime.AsTime(),
		Passed:     pb.Passed,
	}
}

func protoFromTaskResult(m *TaskResult) *TaskResultData {
	return &TaskResultData{
		RunId:      m.RunID,
		HookRunId:  m.HookRunID,
		HookId:     m.HookID,
		ActionName: m.ActionName,
		StartTime:  timestamppb.New(m.StartTime),
		EndTime:    timestamppb.New(m.EndTime),
		Passed:     m.Passed,
	}
}

func baseActionsPath(repoID string) string {
	return kv.FormatPath(reposPrefix, repoID)
}

func TasksPath(repoID, runID string) string {
	return kv.FormatPath(baseActionsPath(repoID), tasksPrefix, runID)
}

func RunPath(repoID, runID string) []byte {
	return []byte(kv.FormatPath(baseActionsPath(repoID), runsPrefix, runID))
}

func byBranchPath(repoID, branchID string) string {
	return kv.FormatPath(baseActionsPath(repoID), branchPrefix, branchID)
}

func byCommitPath(repoID, commitID string) string {
	return kv.FormatPath(baseActionsPath(repoID), commitPrefix, commitID)
}

func RunByBranchPath(repoID, branchID, runID string) []byte {
	return []byte(kv.FormatPath(byBranchPath(repoID, branchID), runID))
}

func RunByCommitPath(repoID, commitID, runID string) []byte {
	return []byte(kv.FormatPath(byCommitPath(repoID, commitID), runID))
}

type Service interface {
	Stop()
	Run(ctx context.Context, record graveler.HookRecord) error
	UpdateCommitID(ctx context.Context, repositoryID string, storageNamespace string, runID string, commitID string) error
	GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error)
	GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error)
	ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (RunResultIterator, error)
	ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error)
	graveler.HooksHandler
}

func NewService(ctx context.Context, store Store, source Source, writer OutputWriter, idGen IDGenerator, stats stats.Collector, runHooks bool) *StoreService {
	ctx, cancel := context.WithCancel(ctx)
	return &StoreService{
		Store:    store,
		Source:   source,
		Writer:   writer,
		ctx:      ctx,
		idGen:    idGen,
		cancel:   cancel,
		wg:       sync.WaitGroup{},
		stats:    stats,
		runHooks: runHooks,
	}
}

func (s *StoreService) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *StoreService) asyncRun(record graveler.HookRecord) {
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
func (s *StoreService) Run(ctx context.Context, record graveler.HookRecord) error {
	if !s.runHooks {
		logging.Default().WithField("record", record).Debug("Hooks are disabled, skipping hooks execution")
		return nil
	}

	// load relevant actions
	spec := MatchSpec{
		EventType: record.EventType,
		BranchID:  record.BranchID,
	}
	logging.Default().WithFields(logging.Fields{"record": record, "spec": spec}).Debug("Filtering actions")
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

func (s *StoreService) loadMatchedActions(ctx context.Context, record graveler.HookRecord, spec MatchSpec) ([]*Action, error) {
	actions, err := LoadActions(ctx, s.Source, record)
	if err != nil {
		return nil, err
	}
	return MatchedActions(actions, spec)
}

func (s *StoreService) allocateTasks(runID string, actions []*Action) ([][]*Task, error) {
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

func (s *StoreService) runTasks(ctx context.Context, record graveler.HookRecord, tasks [][]*Task) error {
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
				buf := bytes.Buffer{}
				task.StartTime = time.Now().UTC()

				task.Err = task.Hook.Run(ctx, record, &buf)
				task.EndTime = time.Now().UTC()

				s.stats.CollectEvent(stats.Event{Class: "actions_service", Name: string(record.EventType)})

				if task.Err != nil {
					_, _ = fmt.Fprintf(&buf, "Error: %s\n", task.Err)
					// wrap error with more information
					task.Err = fmt.Errorf("hook run id '%s' failed on action '%s' hook '%s': %w",
						task.HookRunID, task.Action.Name, task.HookID, task.Err)
				}

				err := hookOutputWriter.OutputWrite(ctx, &buf, int64(buf.Len()))
				if err != nil {
					return fmt.Errorf("failed to write action log. Run id '%s' action '%s' hook '%s': %w",
						task.HookRunID, task.Action.Name, task.HookID, err)
				}
				if task.Err != nil {
					// stop execution of tasks and return error
					return task.Err
				}
			}
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
}

func (s *StoreService) saveRunInformation(ctx context.Context, record graveler.HookRecord, tasks [][]*Task) error {
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

func (s *StoreService) saveRunManifestObjectStore(ctx context.Context, manifest RunManifest, storageNamespace string, runID string) error {
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal run manifest: %w", err)
	}
	runManifestPath := FormatRunManifestOutputPath(runID)
	manifestReader := bytes.NewReader(manifestJSON)
	manifestSize := int64(len(manifestJSON))
	return s.Writer.OutputWrite(ctx, storageNamespace, runManifestPath, manifestReader, manifestSize)
}

func (s *StoreService) saveRunManifestDB(ctx context.Context, repositoryID graveler.RepositoryID, manifest RunManifest) error {
	return s.Store.saveRunManifest(ctx, repositoryID, manifest)
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
			// record hook run information
			taskStarted := !task.StartTime.IsZero()
			manifest.HooksRun = append(manifest.HooksRun, TaskResult{
				RunID:      task.RunID,
				HookRunID:  task.HookRunID,
				HookID:     task.HookID,
				ActionName: task.Action.Name,
				StartTime:  task.StartTime,
				EndTime:    task.EndTime,
				Passed:     taskStarted && task.Err == nil, // mark skipped tasks as failed
			})
			// keep min run start time using non-skipped tasks
			if manifest.Run.StartTime.IsZero() || (taskStarted && task.StartTime.Before(manifest.Run.StartTime)) {
				manifest.Run.StartTime = task.StartTime
			}
			// keep max run end time
			if manifest.Run.EndTime.IsZero() || task.EndTime.After(manifest.Run.EndTime) {
				manifest.Run.EndTime = task.EndTime
			}
			// did we fail
			manifest.Run.Passed = manifest.Run.Passed && task.Err == nil
		}
	}

	return manifest
}

// UpdateCommitID assume record is a post event, we use the PreRunID to update the commit_id and save the run manifest again
func (s *StoreService) UpdateCommitID(ctx context.Context, repositoryID string, storageNamespace string, runID string, commitID string) error {
	manifest, err := s.Store.UpdateCommitID(ctx, repositoryID, runID, commitID)
	if err != nil {
		return fmt.Errorf("updating commit ID: %w", err)
	}
	if manifest == nil {
		return nil
	}

	// update manifest
	return s.saveRunManifestObjectStore(ctx, *manifest, storageNamespace, runID)
}

func (s *StoreService) GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error) {
	return s.Store.GetRunResult(ctx, repositoryID, runID)
}

func (s *StoreService) GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error) {
	return s.Store.GetTaskResult(ctx, repositoryID, runID, hookRunID)
}

func (s *StoreService) ListRunResults(ctx context.Context, repositoryID string, branchID, commitID string, after string) (RunResultIterator, error) {
	return s.Store.ListRunResults(ctx, repositoryID, branchID, commitID, after)
}

func (s *StoreService) ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error) {
	return s.Store.ListRunTaskResults(ctx, repositoryID, runID, after)
}

func (s *StoreService) PreCommitHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *StoreService) PostCommitHook(ctx context.Context, record graveler.HookRecord) error {
	// update pre-commit with commit ID if needed
	err := s.UpdateCommitID(ctx, record.RepositoryID.String(), record.StorageNamespace.String(), record.PreRunID, record.CommitID.String())
	if err != nil {
		return err
	}

	s.asyncRun(record)
	return nil
}

func (s *StoreService) PreMergeHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *StoreService) PostMergeHook(ctx context.Context, record graveler.HookRecord) error {
	// update pre-merge with commit ID if needed
	err := s.UpdateCommitID(ctx, record.RepositoryID.String(), record.StorageNamespace.String(), record.PreRunID, record.CommitID.String())
	if err != nil {
		return err
	}

	s.asyncRun(record)
	return nil
}

func (s *StoreService) PreCreateTagHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *StoreService) PostCreateTagHook(_ context.Context, record graveler.HookRecord) {
	s.asyncRun(record)
}

func (s *StoreService) PreDeleteTagHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *StoreService) PostDeleteTagHook(_ context.Context, record graveler.HookRecord) {
	s.asyncRun(record)
}

func (s *StoreService) PreCreateBranchHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *StoreService) PostCreateBranchHook(_ context.Context, record graveler.HookRecord) {
	s.asyncRun(record)
}

func (s *StoreService) PreDeleteBranchHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *StoreService) PostDeleteBranchHook(_ context.Context, record graveler.HookRecord) {
	s.asyncRun(record)
}

func (s *StoreService) NewRunID() string {
	return s.idGen.NewRunID()
}

func NewHookRunID(actionIdx, hookIdx int) string {
	return fmt.Sprintf("%04d_%04d", actionIdx, hookIdx)
}
