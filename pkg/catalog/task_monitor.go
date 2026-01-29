package catalog

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// Task ID prefixes for async operations not defined in catalog.go
	commitAsyncTaskIDPrefix = "CA"
	mergeAsyncTaskIDPrefix  = "MA"

	// Operation names for metrics labels
	opCommit           = "commit"
	opMerge            = "merge"
	opDumpRefs         = "dump_refs"
	opRestoreRefs      = "restore_refs"
	opGCPrepareCommits = "gc_prepare_commits"

	statusSuccess  = "success"
	statusFailure  = "failure"
	statusExpired  = "expired"  // Task completed but exceeded client-facing deadline
	statusOrphaned = "orphaned" // Task heartbeat stopped and was cleaned up

	// Log field keys
	taskIDFieldKey    = "task_id"
	operationFieldKey = "operation"
	userIDFieldKey    = "user_id"
	statusFieldKey    = "status"
)

var (
	asyncOperationsRunning = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_async_operations_running",
			Help: "Number of async operations currently running",
		},
		[]string{"operation"},
	)

	asyncOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lakefs_async_operations_total",
			Help: "Total number of completed async operations",
		},
		[]string{"operation", "status"},
	)

	asyncOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lakefs_async_operation_duration_seconds",
			Help:    "Duration of async operations from start to completion",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		},
		[]string{"operation"},
	)
)

// TaskMonitor tracks async task lifecycle, recording metrics and performing logging.
type TaskMonitor struct {
	logger   logging.Logger
	logLevel logrus.Level
}

// NewTaskMonitor creates a new TaskMonitor with the given logger and audit log level.
func NewTaskMonitor(logger logging.Logger, auditLogLevel string, isAdvancedAuth bool) *TaskMonitor {
	if logger == nil {
		logger = logging.ContextUnavailable()
	}
	level, err := logrus.ParseLevel(strings.ToLower(auditLogLevel))
	if err != nil {
		level = logrus.DebugLevel
	}
	loggerWithFields := logger.WithField(logging.ServiceNameFieldKey, "task_monitor")
	if isAdvancedAuth {
		loggerWithFields = loggerWithFields.WithField(logging.LogAudit, true)
	}
	return &TaskMonitor{
		logger:   loggerWithFields,
		logLevel: level,
	}
}

func DefaultTaskMonitor() *TaskMonitor {
	return NewTaskMonitor(nil, "debug", false)
}

// TaskSpan is returned by StartSpan and should be deferred to record completion.
// End() determines success/failure by inspecting the task's StatusCode and ErrorMsg
// fields, so these must be set before End() is called.
type TaskSpan struct {
	monitor   *TaskMonitor
	ctx       context.Context
	taskID    string
	operation string
	repoID    string
	task      *Task
	start     time.Time
}

// End records task completion metrics and logs. Call via defer.
func (ts *TaskSpan) End() {
	if ts.operation == "" || ts.task == nil {
		return
	}

	asyncOperationsRunning.WithLabelValues(ts.operation).Dec()

	status := statusSuccess
	if ts.task.StatusCode == http.StatusRequestTimeout {
		status = statusExpired
	} else if ts.task.ErrorMsg != "" {
		status = statusFailure
	}

	asyncOperationsTotal.WithLabelValues(ts.operation, status).Inc()
	asyncOperationDuration.WithLabelValues(ts.operation).Observe(time.Since(ts.start).Seconds())

	ts.monitor.logEvent(ts.ctx, ts.taskID, ts.operation, ts.repoID, "Task completed", status)
}

// StartSpan logs task start, increments in-flight gauge, and returns a span.
// The returned TaskSpan.End() should be deferred to record completion.
func (m *TaskMonitor) StartSpan(ctx context.Context, task *Task, repoID string) *TaskSpan {
	if task == nil {
		return &TaskSpan{}
	}
	taskID := task.Id
	operation := m.taskIDToOperation(taskID)
	if operation == "" {
		return &TaskSpan{}
	}

	asyncOperationsRunning.WithLabelValues(operation).Inc()
	m.logEvent(ctx, taskID, operation, repoID, "Task started", "")

	return &TaskSpan{
		monitor:   m,
		ctx:       ctx,
		taskID:    taskID,
		operation: operation,
		repoID:    repoID,
		task:      task,
		start:     time.Now(),
	}
}

// RecordOrphanedTask records metrics for tasks with a stopped heartbeat that were cleaned up.
// Call this from cleanup jobs for tasks that were never status-checked or whose execution was interrupted.
func (m *TaskMonitor) RecordOrphanedTask(ctx context.Context, taskID, repoID string) {
	operation := m.taskIDToOperation(taskID)
	if operation == "" {
		return
	}
	asyncOperationsTotal.WithLabelValues(operation, statusOrphaned).Inc()
	m.logEvent(ctx, taskID, operation, repoID, "Orphaned task cleaned up", statusOrphaned)
}

// logEvent logs a task lifecycle event with standard fields.
func (m *TaskMonitor) logEvent(ctx context.Context, taskID, operation, repoID, message, status string) {
	fields := logging.Fields{
		taskIDFieldKey:    taskID,
		operationFieldKey: operation,
	}
	if repoID != "" {
		fields[logging.RepositoryFieldKey] = repoID
	}
	if status != "" {
		fields[statusFieldKey] = status
	}
	if ctx != nil {
		if user, err := auth.GetUser(ctx); err == nil && user != nil {
			fields[userIDFieldKey] = user.Username
		}
		if reqID := httputil.RequestIDFromContext(ctx); reqID != nil {
			fields[logging.RequestIDFieldKey] = *reqID
		}
	}
	m.logger.WithFields(fields).Log(m.logLevel, message)
}

func (m *TaskMonitor) taskIDToOperation(taskID string) string {
	switch {
	case strings.HasPrefix(taskID, commitAsyncTaskIDPrefix):
		return opCommit
	case strings.HasPrefix(taskID, mergeAsyncTaskIDPrefix):
		return opMerge
	case strings.HasPrefix(taskID, DumpRefsTaskIDPrefix):
		return opDumpRefs
	case strings.HasPrefix(taskID, RestoreRefsTaskIDPrefix):
		return opRestoreRefs
	case strings.HasPrefix(taskID, GarbageCollectionPrepareCommitsPrefix):
		return opGCPrepareCommits
	default:
		m.logger.WithField(taskIDFieldKey, taskID).Debug("Unknown task ID prefix, skipping metrics")
		return ""
	}
}
