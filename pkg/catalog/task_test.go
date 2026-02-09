package catalog

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testTaskPrefix = "Test"
	syncTestSleep  = 500 * time.Millisecond
)

// TestRunBackgroundTaskSteps_InstanceHeartbeat verifies that the instance heartbeat
// is written periodically and that tasks record the owner instance ID.
func TestRunBackgroundTaskSteps_InstanceHeartbeat(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &TaskMsg{}

		taskDuration := TaskHeartbeatInterval * 3
		steps := []TaskStep{
			{
				Name: "long running task",
				Func: func(ctx context.Context) error {
					time.Sleep(taskDuration)
					return nil
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err)

		// Verify task has the owner instance ID
		var status TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
		require.NoError(t, err)
		require.NotNil(t, status.Task)
		require.Equal(t, c.instanceID, status.Task.OwnerInstanceId)

		// Verify instance heartbeat is written
		time.Sleep(TaskHeartbeatInterval + syncTestSleep)
		synctest.Wait()

		var hb InstanceHeartbeat
		_, err = kv.GetMsg(ctx, kvStore, instancesPartition, instanceHeartbeatPath(c.instanceID), &hb)
		require.NoError(t, err)
		firstUpdate := hb.UpdatedAt.AsTime()

		// Verify heartbeat is updated on next tick
		time.Sleep(TaskHeartbeatInterval)
		synctest.Wait()

		_, err = kv.GetMsg(ctx, kvStore, instancesPartition, instanceHeartbeatPath(c.instanceID), &hb)
		require.NoError(t, err)
		secondUpdate := hb.UpdatedAt.AsTime()
		require.True(t, secondUpdate.After(firstUpdate),
			"second heartbeat timestamp (%v) should be after first (%v)", secondUpdate, firstUpdate)

		// Wait for task to complete
		time.Sleep(taskDuration)
		synctest.Wait()

		var finalStatus TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &finalStatus)
		require.NoError(t, err)
		require.True(t, finalStatus.Task.Done)
	})
}

// TestRunBackgroundTaskSteps_StatusReadableDuringExecution verifies that
// status can be read correctly at any time during task execution.
func TestRunBackgroundTaskSteps_StatusReadableDuringExecution(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &TaskMsg{}

		taskDuration := TaskHeartbeatInterval * 4
		steps := []TaskStep{
			{
				Name: "long task",
				Func: func(ctx context.Context) error {
					time.Sleep(taskDuration)
					return nil
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err)

		// Read status multiple times during execution
		readInterval := TaskHeartbeatInterval / 2
		numReads := 6

		for i := 0; i < numReads; i++ {
			time.Sleep(readInterval)
			synctest.Wait()

			var status TaskMsg
			_, err := GetTaskStatus(ctx, kvStore, repository, taskID, &status)
			require.NoError(t, err, "read %d: failed to parse status", i+1)

			require.NotNil(t, status.Task, "read %d: task should not be nil", i+1)
			require.Equal(t, taskID, status.Task.Id, "read %d: task ID mismatch", i+1)
		}

		// Wait for task to complete before test ends
		time.Sleep(TaskHeartbeatInterval * 2)
		synctest.Wait()
	})
}

// TestRunBackgroundTaskSteps_TaskLifecycle verifies the complete task lifecycle:
// - Task starts with correct fields
// - Task UpdatedAt is set by executeTaskSteps on completion
// - Timestamp does not change after task completion
func TestRunBackgroundTaskSteps_TaskLifecycle(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &TaskMsg{}

		taskDuration := 5 * time.Second
		steps := []TaskStep{
			{
				Name: "long task",
				Func: func(ctx context.Context) error {
					time.Sleep(taskDuration)
					return nil
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err)

		// After 2 seconds, task should still be running
		time.Sleep(2 * time.Second)
		synctest.Wait()

		var statusDuring TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusDuring)
		require.NoError(t, err)
		require.NotNil(t, statusDuring.Task)
		require.False(t, statusDuring.Task.Done, "task should still be running after 2 seconds")
		require.NotNil(t, statusDuring.Task.UpdatedAt)

		// Wait for task to complete
		time.Sleep(4 * time.Second)
		synctest.Wait()

		var statusAfterCompletion TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusAfterCompletion)
		require.NoError(t, err)
		require.True(t, statusAfterCompletion.Task.Done, "task should be done")
		require.NotNil(t, statusAfterCompletion.Task.UpdatedAt)
		completionTime := statusAfterCompletion.Task.UpdatedAt.AsTime()

		// Wait 2 more seconds and verify timestamp is NOT updated anymore
		time.Sleep(2 * time.Second)
		synctest.Wait()

		var statusAfterWait TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusAfterWait)
		require.NoError(t, err)
		require.True(t, statusAfterWait.Task.Done)
		finalTimestamp := statusAfterWait.Task.UpdatedAt.AsTime()

		require.Equal(t, completionTime.UnixNano(), finalTimestamp.UnixNano(),
			"timestamp should NOT change after task completion (completion: %v, after 2s wait: %v)",
			completionTime, finalTimestamp)
	})
}

// TestRunBackgroundTaskSteps_TaskFailure verifies that errors are properly
// captured in the task status
func TestRunBackgroundTaskSteps_TaskFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &TaskMsg{}

		expectedErr := errors.New("task failed")
		steps := []TaskStep{
			{
				Name: "failing task",
				Func: func(ctx context.Context) error {
					return expectedErr
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err) // RunBackgroundTaskSteps itself should not error

		time.Sleep(syncTestSleep)
		synctest.Wait()

		var status TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
		require.NoError(t, err)
		require.True(t, status.Task.Done)
		require.NotEmpty(t, status.Task.ErrorMsg)
	})
}

// TestRunBackgroundTaskSteps_WithStatusData verifies that task-specific
// status data is preserved through task completion
func TestRunBackgroundTaskSteps_WithStatusData(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &CommitAsyncStatusData{}

		expectedCommitID := "abc123def456"
		steps := []TaskStep{
			{
				Name: "task with result",
				Func: func(ctx context.Context) error {
					time.Sleep(1 * time.Second)
					taskStatus.Info = &graveler.CommitData{
						Id:           expectedCommitID,
						Message:      "test commit",
						Committer:    "test-user",
						CreationDate: timestamppb.Now(),
					}
					return nil
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err)

		time.Sleep(syncTestSleep)
		synctest.Wait()

		var statusDuring CommitAsyncStatusData
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusDuring)
		require.NoError(t, err)
		require.Nil(t, statusDuring.Info, "Info should be nil while task is running")

		time.Sleep(1 * time.Second)
		synctest.Wait()

		var statusAfter CommitAsyncStatusData
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusAfter)
		require.NoError(t, err)
		require.True(t, statusAfter.Task.Done)
		require.NotNil(t, statusAfter.Info)
		require.Equal(t, expectedCommitID, statusAfter.Info.Id)
	})
}

// TestGetValidatedTaskStatus_InstanceHeartbeatExpiry verifies task expiry behavior
// based on instance heartbeat staleness
func TestGetValidatedTaskStatus_InstanceHeartbeatExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		expiryDuration time.Duration
		heartbeatAge   time.Duration
		expectExpired  bool
	}{
		{
			name:           "expired - instance heartbeat exceeds expiry window",
			expiryDuration: 10 * time.Minute,
			heartbeatAge:   11 * time.Minute,
			expectExpired:  true,
		},
		{
			name:           "not expired - instance heartbeat within expiry window",
			expiryDuration: 10 * time.Minute,
			heartbeatAge:   5 * time.Minute,
			expectExpired:  false,
		},
		{
			name:           "zero expiry - never expires",
			expiryDuration: 0,
			heartbeatAge:   1 * time.Hour,
			expectExpired:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const taskTime = 1 * time.Hour
			synctest.Test(t, func(t *testing.T) {
				t.Cleanup(func() {
					// Let the task goroutine finish
					time.Sleep(taskTime)
					synctest.Wait()
				})

				kvStore, c, repository := setupTaskTest(t)
				ctx := t.Context()

				taskID := NewTaskID(testTaskPrefix)
				taskStatus := &TaskMsg{}

				steps := []TaskStep{
					{
						Name: "long task",
						Func: func(ctx context.Context) error {
							time.Sleep(taskTime)
							return nil
						},
					},
				}

				err := c.RunBackgroundTaskSteps(ctx, repository, "operation", taskID, steps, taskStatus)
				require.NoError(t, err)

				time.Sleep(syncTestSleep)
				synctest.Wait()

				// Simulate stale instance heartbeat by overwriting the heartbeat entry
				hb := &InstanceHeartbeat{
					UpdatedAt: timestamppb.New(time.Now().Add(-tt.heartbeatAge)),
				}
				err = kv.SetMsg(ctx, kvStore, instancesPartition, instanceHeartbeatPath(c.instanceID), hb)
				require.NoError(t, err)

				// Use a different catalog instance to check status (simulates another server checking)
				checker := checkerCatalog(kvStore, repository)
				var resultStatus TaskMsg
				err = checker.GetValidatedTaskStatus(ctx, repository.RepositoryID.String(), taskID, testTaskPrefix, &resultStatus, tt.expiryDuration)
				require.NoError(t, err)

				if tt.expectExpired {
					require.True(t, resultStatus.Task.Done)
					require.Contains(t, resultStatus.Task.ErrorMsg, "expired")
					require.Equal(t, http.StatusRequestTimeout, int(resultStatus.Task.StatusCode))
				} else {
					require.False(t, resultStatus.Task.Done)
					require.Empty(t, resultStatus.Task.ErrorMsg)
				}
			})
		})
	}
}

// TestGetValidatedTaskStatus_MissingInstanceHeartbeat verifies that a task whose
// owner instance heartbeat entry is missing is marked as expired.
func TestGetValidatedTaskStatus_MissingInstanceHeartbeat(t *testing.T) {
	t.Parallel()
	const taskTime = 1 * time.Hour
	synctest.Test(t, func(t *testing.T) {
		t.Cleanup(func() {
			time.Sleep(taskTime)
			synctest.Wait()
		})

		kvStore, c, repository := setupTaskTest(t)
		ctx := t.Context()

		taskID := NewTaskID(testTaskPrefix)
		taskStatus := &TaskMsg{}

		steps := []TaskStep{
			{
				Name: "long task",
				Func: func(ctx context.Context) error {
					time.Sleep(taskTime)
					return nil
				},
			},
		}

		err := c.RunBackgroundTaskSteps(ctx, repository, "operation", taskID, steps, taskStatus)
		require.NoError(t, err)

		time.Sleep(syncTestSleep)
		synctest.Wait()

		// Delete the instance heartbeat to simulate instance gone
		err = kvStore.Delete(ctx, []byte(instancesPartition), instanceHeartbeatPath(c.instanceID))
		require.NoError(t, err)

		// Use a different catalog instance to check status (simulates another server checking)
		checker := checkerCatalog(kvStore, repository)
		var resultStatus TaskMsg
		err = checker.GetValidatedTaskStatus(ctx, repository.RepositoryID.String(), taskID, testTaskPrefix, &resultStatus, 10*time.Minute)
		require.NoError(t, err)
		require.True(t, resultStatus.Task.Done)
		require.Contains(t, resultStatus.Task.ErrorMsg, "no longer available")
		require.Equal(t, http.StatusRequestTimeout, int(resultStatus.Task.StatusCode))
	})
}

// TestGetValidatedTaskStatus_LegacyTaskFallback verifies that tasks without
// OwnerInstanceId (created by older lakeFS versions) fall back to UpdatedAt-based expiry.
func TestGetValidatedTaskStatus_LegacyTaskFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		expiryDuration time.Duration
		timeAdvance    time.Duration
		expectExpired  bool
	}{
		{
			name:           "legacy expired",
			expiryDuration: 10 * time.Minute,
			timeAdvance:    11 * time.Minute,
			expectExpired:  true,
		},
		{
			name:           "legacy not expired",
			expiryDuration: 10 * time.Minute,
			timeAdvance:    5 * time.Minute,
			expectExpired:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				kvStore, c, repository := setupTaskTest(t)
				ctx := t.Context()

				// Create a legacy task directly (no OwnerInstanceId)
				taskID := NewTaskID(testTaskPrefix)
				legacyTask := &TaskMsg{
					Task: &Task{
						Id:        taskID,
						Operation: "legacy_op",
						UpdatedAt: timestamppb.New(time.Now().Add(-tt.timeAdvance)),
					},
				}
				err := UpdateTaskStatus(ctx, kvStore, repository, taskID, legacyTask)
				require.NoError(t, err)

				var resultStatus TaskMsg
				err = c.GetValidatedTaskStatus(ctx, repository.RepositoryID.String(), taskID, testTaskPrefix, &resultStatus, tt.expiryDuration)
				require.NoError(t, err)

				if tt.expectExpired {
					require.True(t, resultStatus.Task.Done)
					require.Contains(t, resultStatus.Task.ErrorMsg, "expired")
					require.Equal(t, http.StatusRequestTimeout, int(resultStatus.Task.StatusCode))
				} else {
					require.False(t, resultStatus.Task.Done)
					require.Empty(t, resultStatus.Task.ErrorMsg)
				}
			})
		})
	}
}

// setupTaskTest creates the common test setup used by all task tests.
// It creates a Catalog with an instance ID and starts the instance heartbeat.
func setupTaskTest(t *testing.T) (kv.Store, *Catalog, *graveler.RepositoryRecord) {
	t.Helper()
	ctx := t.Context()

	// kv store
	kvStore := kvtest.GetStore(ctx, t)

	// repository
	repository := &graveler.RepositoryRecord{
		RepositoryID: "test-repo",
		Repository: &graveler.Repository{
			StorageID:        "test-storage",
			StorageNamespace: "s3://test-bucket",
			DefaultBranchID:  "main",
		},
	}

	// fake graveler store that returns the test repository
	fakeStore := &fakeGravelerForTaskTest{
		repository: repository,
	}

	// catalog with instance heartbeat
	workPool := pond.NewPool(sharedWorkers, pond.WithContext(ctx))
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	catalog := &Catalog{
		KVStore:    kvStore,
		Store:      fakeStore,
		workPool:   workPool,
		instanceID: xid.New().String(),
		errorToStatusCodeAndMsg: func(logger logging.Logger, err error) (int, string, bool) {
			return http.StatusInternalServerError, err.Error(), true
		},
	}
	go catalog.runInstanceHeartbeat(heartbeatCtx)
	t.Cleanup(func() {
		heartbeatCancel()
	})

	return kvStore, catalog, repository
}

// fakeGravelerForTaskTest is a minimal fake that only implements GetRepository
type fakeGravelerForTaskTest struct {
	FakeGraveler
	repository *graveler.RepositoryRecord
}

func (f *fakeGravelerForTaskTest) GetRepository(_ context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	if f.repository != nil && f.repository.RepositoryID == repositoryID {
		return f.repository, nil
	}
	return nil, graveler.ErrNotFound
}

// checkerCatalog returns a minimal Catalog with a distinct instanceID, used to
// call GetValidatedTaskStatus from a "different server" than the task owner.
func checkerCatalog(kvStore kv.Store, repository *graveler.RepositoryRecord) *Catalog {
	return &Catalog{
		KVStore:    kvStore,
		Store:      &fakeGravelerForTaskTest{repository: repository},
		instanceID: xid.New().String(),
	}
}
