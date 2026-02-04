package catalog

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alitto/pond/v2"
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

// TestRunBackgroundTaskSteps_HeartbeatUpdatesTimestamp verifies that the heartbeat
// updates the task's UpdatedAt timestamp periodically
func TestRunBackgroundTaskSteps_HeartbeatUpdatesTimestamp(t *testing.T) {
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

		time.Sleep(TaskHeartbeatInterval + syncTestSleep)
		synctest.Wait()

		var status TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
		require.NoError(t, err)
		require.NotNil(t, status.Task)
		require.False(t, status.Task.Done)
		require.NotNil(t, status.Task.UpdatedAt, "first UpdatedAt should not be nil")
		firstUpdate := status.Task.UpdatedAt.AsTime()

		time.Sleep(TaskHeartbeatInterval)
		synctest.Wait()

		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
		require.NoError(t, err)
		require.NotNil(t, status.Task)
		require.False(t, status.Task.Done)
		require.NotNil(t, status.Task.UpdatedAt, "second UpdatedAt should not be nil")
		secondUpdate := status.Task.UpdatedAt.AsTime()

		require.True(t, secondUpdate.After(firstUpdate),
			"second heartbeat timestamp (%v) should be after first (%v)",
			secondUpdate, firstUpdate)

		time.Sleep(taskDuration)
		synctest.Wait()

		var finalStatus TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &finalStatus)
		require.NoError(t, err)
		require.NotNil(t, finalStatus.Task)
		require.True(t, finalStatus.Task.Done)
	})
}

// TestRunBackgroundTaskSteps_HeartbeatWritesFullStatus verifies that the heartbeat
// writes the full status message (TaskMsg), not just the Task struct.
// This is a regression test for the bug where heartbeat wrote only Task instead of
// the full status, causing "proto: cannot parse invalid wire-format data" errors.
func TestRunBackgroundTaskSteps_HeartbeatWritesFullStatus(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &TaskMsg{}

		steps := []TaskStep{
			{
				Name: "test task",
				Func: func(ctx context.Context) error {
					time.Sleep(TaskHeartbeatInterval * 2)
					return nil
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err)

		time.Sleep(TaskHeartbeatInterval + syncTestSleep)
		synctest.Wait()

		// Try to read the status as TaskMsg (not just Task)
		// This should NOT fail with "proto: cannot parse invalid wire-format data"
		var readStatus TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &readStatus)
		require.NoError(t, err, "should be able to parse TaskMsg after heartbeat update")

		require.NotNil(t, readStatus.Task)
		require.Equal(t, taskID, readStatus.Task.Id)
		require.False(t, readStatus.Task.Done)
		require.NotNil(t, readStatus.Task.UpdatedAt)

		// Wait for task to complete before test ends
		time.Sleep(TaskHeartbeatInterval)
		synctest.Wait()
	})
}

// TestRunBackgroundTaskSteps_StatusReadableDuringHeartbeat verifies that
// status can be read correctly at any time during heartbeat updates
func TestRunBackgroundTaskSteps_StatusReadableDuringHeartbeat(t *testing.T) {
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

		// Read status multiple times during heartbeat updates
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

// TestRunBackgroundTaskSteps_HeartbeatLifecycle verifies the complete heartbeat lifecycle:
// - Timestamp IS updated during task execution
// - Timestamp stops updating after task completion
func TestRunBackgroundTaskSteps_HeartbeatLifecycle(t *testing.T) {
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

		// After 2 seconds, task should still be running and timestamp should be updated
		time.Sleep(2 * time.Second)
		synctest.Wait()

		var statusDuring TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusDuring)
		require.NoError(t, err)
		require.NotNil(t, statusDuring.Task)
		require.False(t, statusDuring.Task.Done, "task should still be running after 2 seconds")
		require.NotNil(t, statusDuring.Task.UpdatedAt)
		timestampDuringExecution := statusDuring.Task.UpdatedAt.AsTime()

		// Wait for task to complete (3 more seconds + buffer)
		time.Sleep(4 * time.Second)
		synctest.Wait()

		var statusAfterCompletion TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusAfterCompletion)
		require.NoError(t, err)
		require.True(t, statusAfterCompletion.Task.Done, "task should be done")
		require.NotNil(t, statusAfterCompletion.Task.UpdatedAt)
		completionTime := statusAfterCompletion.Task.UpdatedAt.AsTime()

		// Verify timestamp was updated during execution
		require.True(t, completionTime.After(timestampDuringExecution) || completionTime.Equal(timestampDuringExecution),
			"completion timestamp (%v) should be after or equal to timestamp during execution (%v)",
			completionTime, timestampDuringExecution)

		// Wait 2 more seconds and verify timestamp is NOT updated anymore
		time.Sleep(2 * time.Second)
		synctest.Wait()

		var statusAfterWait TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusAfterWait)
		require.NoError(t, err)
		require.True(t, statusAfterWait.Task.Done)
		require.NotNil(t, statusAfterWait.Task.UpdatedAt)
		finalTimestamp := statusAfterWait.Task.UpdatedAt.AsTime()

		require.Equal(t, completionTime.UnixNano(), finalTimestamp.UnixNano(),
			"timestamp should NOT change after task completion (completion: %v, after 2s wait: %v)",
			completionTime, finalTimestamp)
	})
}

// TestRunBackgroundTaskSteps_HeartbeatStopsWhenDone verifies that
// the heartbeat stops updating once the task is marked as done
func TestRunBackgroundTaskSteps_HeartbeatStopsWhenDone(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		kvStore, c, repository := setupTaskTest(t)

		taskID := NewTaskID("TEST")
		taskStatus := &TaskMsg{}

		steps := []TaskStep{
			{
				Name: "quick task",
				Func: func(ctx context.Context) error {
					time.Sleep(syncTestSleep)
					return nil
				},
			},
		}

		ctx := t.Context()
		err := c.RunBackgroundTaskSteps(ctx, repository, OpDumpRefs, taskID, steps, taskStatus)
		require.NoError(t, err)

		time.Sleep(1 * time.Second)
		synctest.Wait()

		var status TaskMsg
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
		require.NoError(t, err)
		require.True(t, status.Task.Done)
		completionTime := status.Task.UpdatedAt.AsTime()

		time.Sleep(TaskHeartbeatInterval + syncTestSleep)
		synctest.Wait()

		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
		require.NoError(t, err)
		require.True(t, status.Task.Done)
		require.Equal(t, completionTime.UnixNano(), status.Task.UpdatedAt.AsTime().UnixNano(),
			"timestamp should not change after task completion")
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
					time.Sleep(TaskHeartbeatInterval * 2)
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

		time.Sleep(TaskHeartbeatInterval + syncTestSleep)
		synctest.Wait()

		var statusDuring CommitAsyncStatusData
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusDuring)
		require.NoError(t, err)
		require.Nil(t, statusDuring.Info, "Info should be nil while task is running")

		time.Sleep(TaskHeartbeatInterval + 1*time.Second)
		synctest.Wait()

		var statusAfter CommitAsyncStatusData
		_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &statusAfter)
		require.NoError(t, err)
		require.True(t, statusAfter.Task.Done)
		require.NotNil(t, statusAfter.Info)
		require.Equal(t, expectedCommitID, statusAfter.Info.Id)
	})
}

// TestGetValidatedTaskStatus_Expiry verifies task expiry behavior through the public interface
func TestGetValidatedTaskStatus_Expiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		expiryDuration time.Duration
		timeAdvance    time.Duration
		expectExpired  bool
	}{
		{
			name:           "expired - exceeds expiry window",
			expiryDuration: 10 * time.Minute,
			timeAdvance:    11 * time.Minute,
			expectExpired:  true,
		},
		{
			name:           "not expired - within expiry window",
			expiryDuration: 10 * time.Minute,
			timeAdvance:    5 * time.Minute,
			expectExpired:  false,
		},
		{
			name:           "zero expiry - never expires",
			expiryDuration: 0,
			timeAdvance:    1 * time.Hour,
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

				err := c.RunBackgroundTaskSteps(ctx, repository, taskID, steps, taskStatus)
				require.NoError(t, err)

				time.Sleep(syncTestSleep)
				synctest.Wait()

				// Simulate stalled heartbeat by overwriting UpdatedAt
				var status TaskMsg
				_, err = GetTaskStatus(ctx, kvStore, repository, taskID, &status)
				require.NoError(t, err)
				require.False(t, status.Task.Done)
				status.Task.UpdatedAt = timestamppb.New(time.Now().Add(-tt.timeAdvance))
				err = UpdateTaskStatus(ctx, kvStore, repository, taskID, &status)
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

// setupTaskTest creates the common test setup used by all task tests
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

	// catalog
	workPool := pond.NewPool(sharedWorkers, pond.WithContext(ctx))
	catalog := &Catalog{
		KVStore:  kvStore,
		Store:    fakeStore,
		workPool: workPool,
		errorToStatusCodeAndMsg: func(logger logging.Logger, err error) (int, string, bool) {
			return http.StatusInternalServerError, err.Error(), true
		},
	}

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
