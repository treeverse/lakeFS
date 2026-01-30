package catalog

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTaskMonitor_StartSpanAndEnd(t *testing.T) {
	m := NewTaskMonitor(nil, "DEBUG", false)

	t.Run("successful completion increments and decrements gauge, records success", func(t *testing.T) {
		task := &Task{Id: "CA-test-123"}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opCommit, statusSuccess))

		span := m.StartSpan(context.TODO(), task, "test-repo")
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit)),
			"gauge should increment on StartSpan")

		task.Done = true
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit)),
			"gauge should decrement on End")
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opCommit, statusSuccess)),
			"success counter should increment")
	})

	t.Run("failed completion records failure status", func(t *testing.T) {
		task := &Task{Id: "MA-test-456"}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opMerge))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opMerge, statusFailure))

		span := m.StartSpan(context.TODO(), task, "test-repo")
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opMerge)))

		task.Done = true
		task.ErrorMsg = "something went wrong"
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opMerge)))
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opMerge, statusFailure)),
			"failure counter should increment")
	})

	t.Run("expired completion records expired status", func(t *testing.T) {
		task := &Task{Id: "DR-test-789"}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opDumpRefs))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opDumpRefs, statusExpired))

		span := m.StartSpan(context.TODO(), task, "test-repo")
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opDumpRefs)))

		task.Done = true
		task.StatusCode = http.StatusRequestTimeout
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opDumpRefs)))
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opDumpRefs, statusExpired)),
			"expired counter should increment")
	})

	t.Run("unknown task type does not affect metrics", func(t *testing.T) {
		task := &Task{Id: "UNKNOWN-123"}

		// Get baseline for all operations
		commitRunning := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit))
		mergeRunning := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opMerge))

		span := m.StartSpan(context.TODO(), task, "test-repo")
		span.End()

		// No metrics should have changed
		require.Equal(t, commitRunning, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit)))
		require.Equal(t, mergeRunning, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opMerge)))
	})

	t.Run("nil task does not affect metrics", func(t *testing.T) {
		commitRunning := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit))

		span := m.StartSpan(context.TODO(), nil, "test-repo")
		span.End()

		require.Equal(t, commitRunning, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opCommit)))
	})
}

func TestTaskMonitor_RecordOrphanedTask(t *testing.T) {
	m := NewTaskMonitor(nil, "DEBUG", false)

	t.Run("records orphaned status for known task types", func(t *testing.T) {
		commitBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opCommit, statusOrphaned))
		mergeBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opMerge, statusOrphaned))

		m.RecordOrphanedTask(context.TODO(), "CA-orphaned-123", "test-repo")
		m.RecordOrphanedTask(context.TODO(), "MA-orphaned-456", "test-repo")

		require.Equal(t, commitBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opCommit, statusOrphaned)))
		require.Equal(t, mergeBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opMerge, statusOrphaned)))
	})

	t.Run("unknown task type does not affect metrics", func(t *testing.T) {
		commitBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opCommit, statusOrphaned))

		m.RecordOrphanedTask(context.TODO(), "UNKNOWN-orphaned", "test-repo")

		require.Equal(t, commitBefore, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opCommit, statusOrphaned)))
	})
}

func TestTaskIDToOperation(t *testing.T) {
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
