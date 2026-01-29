package catalog

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskMonitor_RecordLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("commit task lifecycle", func(t *testing.T) {
		m := NewTaskMonitor(nil, "DEBUG", false)
		task := &Task{Id: "CA-test-123"}

		// First call - submitted (pending)
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		tracked := m.tasks[task.Id]
		require.NotNil(t, tracked)
		require.Equal(t, taskPhasePending, tracked.phase)
		require.Equal(t, opCommit, tracked.operation)
		m.mu.Unlock()

		// Second call (still not done) - started (running)
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		require.Equal(t, taskPhaseRunning, tracked.phase)
		m.mu.Unlock()

		// Third call with Done=true - completed
		task.Done = true
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		require.Equal(t, taskPhaseCompleted, tracked.phase)
		m.mu.Unlock()

		// Fourth call - should be ignored (terminal state)
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		require.Equal(t, taskPhaseCompleted, tracked.phase)
		m.mu.Unlock()
	})

	t.Run("merge task lifecycle", func(t *testing.T) {
		m := NewTaskMonitor(nil, "DEBUG", false)
		task := &Task{Id: "MA-test-456"}

		// First call - submitted
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		tracked := m.tasks[task.Id]
		require.Equal(t, opMerge, tracked.operation)
		m.mu.Unlock()
	})

	t.Run("expired task", func(t *testing.T) {
		m := NewTaskMonitor(nil, "DEBUG", false)
		task := &Task{Id: "CA-test-789"}

		// First call - submitted
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		tracked := m.tasks[task.Id]
		require.Equal(t, taskPhasePending, tracked.phase)
		m.mu.Unlock()

		// Done with timeout status - expired
		task.Done = true
		task.StatusCode = http.StatusRequestTimeout
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		require.Equal(t, taskPhaseExpired, tracked.phase)
		m.mu.Unlock()
	})

	t.Run("failed task", func(t *testing.T) {
		m := NewTaskMonitor(nil, "DEBUG", false)
		task := &Task{Id: "CA-test-fail"}

		// Submit and start
		m.Record(context.TODO(), task, "test-repo")
		m.Record(context.TODO(), task, "test-repo")

		// Complete with error
		task.Done = true
		task.ErrorMsg = "something went wrong"
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		tracked := m.tasks[task.Id]
		require.Equal(t, taskPhaseCompleted, tracked.phase)
		m.mu.Unlock()
	})
}

func TestTaskMonitor_RecordFirstSeenDone(t *testing.T) {
	t.Parallel()

	t.Run("first seen and already completed", func(t *testing.T) {
		m := NewTaskMonitor(nil, "DEBUG", false)
		task := &Task{
			Id:   "CA-first-done",
			Done: true,
		}

		// First call with task already done - should record completion without gauge updates
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		tracked := m.tasks[task.Id]
		require.NotNil(t, tracked)
		require.Equal(t, taskPhaseCompleted, tracked.phase)
		m.mu.Unlock()
	})

	t.Run("first seen and already expired", func(t *testing.T) {
		m := NewTaskMonitor(nil, "DEBUG", false)
		task := &Task{
			Id:         "CA-first-expired",
			Done:       true,
			StatusCode: http.StatusRequestTimeout,
		}

		// First call with task already expired
		m.Record(context.TODO(), task, "test-repo")
		m.mu.Lock()
		tracked := m.tasks[task.Id]
		require.NotNil(t, tracked)
		require.Equal(t, taskPhaseExpired, tracked.phase)
		m.mu.Unlock()
	})
}

func TestTaskMonitor_NilTask(t *testing.T) {
	t.Parallel()
	m := NewTaskMonitor(nil, "DEBUG", false)

	// Should not panic
	m.Record(context.TODO(), nil, "test-repo")

	m.mu.Lock()
	require.Empty(t, m.tasks)
	m.mu.Unlock()
}

func TestTaskMonitor_UnknownTaskPrefix(t *testing.T) {
	t.Parallel()
	m := NewTaskMonitor(nil, "DEBUG", false)

	task := &Task{Id: "UNKNOWN-123"}
	m.Record(context.TODO(), task, "test-repo")

	m.mu.Lock()
	require.Empty(t, m.tasks)
	m.mu.Unlock()
}

func TestTaskMonitor_Cleanup(t *testing.T) {
	t.Parallel()
	m := NewTaskMonitor(nil, "DEBUG", false)

	task := &Task{Id: "CA-cleanup-test"}
	m.Record(context.TODO(), task, "test-repo")

	m.mu.Lock()
	require.Contains(t, m.tasks, task.Id)
	m.mu.Unlock()

	m.Cleanup(task.Id)

	m.mu.Lock()
	require.NotContains(t, m.tasks, task.Id)
	m.mu.Unlock()
}

func TestTaskIDToOperation(t *testing.T) {
	t.Parallel()

	m := NewTaskMonitor(nil, "DEBUG", false)

	tests := []struct {
		taskID   string
		expected string
	}{
		{"CA-123", opCommit},
		{"CA-abc-xyz", opCommit},
		{"MA-456", opMerge},
		{"MA-def-uvw", opMerge},
		{"DR-123", opDumpRefs},
		{"DR-abc-xyz", opDumpRefs},
		{"RR-456", opRestoreRefs},
		{"RR-def-uvw", opRestoreRefs},
		{"GCPC-789", opGCPrepareCommits},
		{"GCPC-ghi-jkl", opGCPrepareCommits},
		{"UNKNOWN-789", ""},
		{"", ""},
		{"C", ""},
		{"M", ""},
	}

	for _, tt := range tests {
		t.Run(tt.taskID, func(t *testing.T) {
			result := m.taskIDToOperation(tt.taskID)
			require.Equal(t, tt.expected, result)
		})
	}
}
