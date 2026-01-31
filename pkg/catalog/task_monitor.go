package catalog

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

type taskPhase int

const (
	taskPhasePending taskPhase = iota
	taskPhaseRunning
	taskPhaseCompleted
	taskPhaseExpired
)

const (
	// Task ID prefixes for async commit and merge (enterprise)
	commitAsyncTaskIDPrefix = "CA"
	mergeAsyncTaskIDPrefix  = "MA"

	// Operation names for metrics labels
	opCommit            = "commit"
	opMerge             = "merge"
	opDumpRefs          = "dump_refs"
	opRestoreRefs       = "restore_refs"
	opGCPrepareCommits  = "gc_prepare_commits"

	statusSuccess = "success"
	statusFailure = "failure"
	statusExpired = "expired"
)

type trackedTask struct {
	phase       taskPhase
	operation   string
	submittedAt time.Time
	startedAt   time.Time
}

var (
	asyncOperationsPending = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_async_operations_pending",
			Help: "Number of async operations submitted but not yet started (may drift after restart)",
		},
		[]string{"operation"},
	)

	asyncOperationsRunning = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_async_operations_running",
			Help: "Number of async operations currently executing (may drift after restart)",
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

	asyncOperationPendingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lakefs_async_operation_pending_duration_seconds",
			Help:    "Time spent in pending state before starting or expiring",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
		[]string{"operation"},
	)

	asyncOperationRunningDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lakefs_async_operation_running_duration_seconds",
			Help:    "Time spent executing before completion or expiration",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		},
		[]string{"operation"},
	)
)

// TaskMonitor tracks task lifecycle and records metrics.
// It infers the task phase from the Task state and call patterns.
type TaskMonitor struct {
	mu       sync.Mutex
	tasks    map[string]*trackedTask
	logger   logging.Logger
	logLevel logrus.Level
}

// NewTaskMonitor creates a new TaskMonitor with the given logger and audit log level.
// If logger is nil, a default logger is used.
// The auditLogLevel should match the configured logging.audit_log_level (e.g., "DEBUG", "INFO").
// If isAdvancedAuth is true, log entries will include the log_audit field for audit filtering.
func NewTaskMonitor(logger logging.Logger, auditLogLevel string, isAdvancedAuth bool) *TaskMonitor {
	if logger == nil {
		logger = logging.ContextUnavailable()
	}
	level, err := logrus.ParseLevel(strings.ToLower(auditLogLevel))
	if err != nil {
		level = logrus.DebugLevel
	}
	loggerWithFields := logger.WithField("service_name", "task_monitor")
	if isAdvancedAuth {
		loggerWithFields = loggerWithFields.WithField(logging.LogAudit, true)
	}
	return &TaskMonitor{
		tasks:    make(map[string]*trackedTask),
		logger:   loggerWithFields,
		logLevel: level,
	}
}

// Record observes a task and records metrics based on inferred lifecycle phase.
// The phase is inferred from:
//   - First call with Done=false → submitted (pending)
//   - Subsequent call with Done=false → started (running)
//   - Call with Done=true and StatusCode=408 → expired
//   - Call with Done=true (other) → completed (success/failure)
//
// The ctx and repository parameters are used for logging context (user, request_id).
// Pass nil ctx and empty repository if context is not available (e.g., cleanup jobs).
func (m *TaskMonitor) Record(ctx context.Context, task *Task, repository string) {
	if task == nil {
		return
	}

	operation := m.taskIDToOperation(task.Id)
	if operation == "" {
		return
	}

	// Build base log fields from context
	logFields := m.buildLogFields(ctx, task.Id, operation, repository)

	m.mu.Lock()
	defer m.mu.Unlock()

	tracked, exists := m.tasks[task.Id]

	// Already in terminal state - ignore
	if exists && (tracked.phase == taskPhaseCompleted || tracked.phase == taskPhaseExpired) {
		return
	}

	now := time.Now()

	if !exists {
		if task.Done {
			// First time seeing this task and it's already done.
			// We missed the lifecycle (e.g., server restart), so just record completion.
			// We can't record accurate pending/running durations.
			isExpired := task.StatusCode == http.StatusRequestTimeout
			status := statusSuccess
			if isExpired {
				status = statusExpired
			} else if task.ErrorMsg != "" {
				status = statusFailure
			}
			asyncOperationsTotal.WithLabelValues(operation, status).Inc()
			phase := taskPhaseCompleted
			if isExpired {
				phase = taskPhaseExpired
			}
			m.tasks[task.Id] = &trackedTask{
				phase:     phase,
				operation: operation,
			}
			logFields["status"] = status
			m.logger.WithFields(logFields).Log(m.logLevel, "Task recorded (missed lifecycle)")
			return
		}
		// First time seeing this task - submitted
		m.tasks[task.Id] = &trackedTask{
			phase:       taskPhasePending,
			operation:   operation,
			submittedAt: now,
		}
		asyncOperationsPending.WithLabelValues(operation).Inc()
		m.logger.WithFields(logFields).Log(m.logLevel, "Task submitted")
		return
	}

	// Task exists in tracking
	if !task.Done {
		// Not done yet - if pending, transition to running
		if tracked.phase == taskPhasePending {
			tracked.phase = taskPhaseRunning
			tracked.startedAt = now
			asyncOperationsPending.WithLabelValues(operation).Dec()
			asyncOperationsRunning.WithLabelValues(operation).Inc()
			asyncOperationPendingDuration.WithLabelValues(operation).Observe(now.Sub(tracked.submittedAt).Seconds())
			m.logger.WithFields(logFields).Log(m.logLevel, "Task started")
		}
		return
	}

	// Task is done - determine if expired or completed
	isExpired := task.StatusCode == http.StatusRequestTimeout
	if isExpired {
		m.recordExpired(tracked, logFields, now)
	} else {
		m.recordCompleted(tracked, task, logFields, now)
	}
}

// buildLogFields creates logging fields from context and task info.
func (m *TaskMonitor) buildLogFields(ctx context.Context, taskID, operation, repository string) logging.Fields {
	fields := logging.Fields{
		"task_id":   taskID,
		"operation": operation,
	}
	if repository != "" {
		fields["repository"] = repository
	}
	if ctx != nil {
		if user, err := auth.GetUser(ctx); err == nil && user != nil {
			fields["user_id"] = user.Username
		}
		if reqID := httputil.RequestIDFromContext(ctx); reqID != nil {
			fields["request_id"] = *reqID
		}
	}
	return fields
}

func (m *TaskMonitor) recordExpired(tracked *trackedTask, logFields logging.Fields, now time.Time) {
	operation := tracked.operation

	if tracked.phase == taskPhasePending {
		asyncOperationsPending.WithLabelValues(operation).Dec()
		asyncOperationPendingDuration.WithLabelValues(operation).Observe(now.Sub(tracked.submittedAt).Seconds())
	} else if tracked.phase == taskPhaseRunning {
		asyncOperationsRunning.WithLabelValues(operation).Dec()
		asyncOperationRunningDuration.WithLabelValues(operation).Observe(now.Sub(tracked.startedAt).Seconds())
	}

	asyncOperationsTotal.WithLabelValues(operation, statusExpired).Inc()
	tracked.phase = taskPhaseExpired

	m.logger.WithFields(logFields).Log(m.logLevel, "Task expired")
}

func (m *TaskMonitor) recordCompleted(tracked *trackedTask, task *Task, logFields logging.Fields, now time.Time) {
	operation := tracked.operation
	status := statusSuccess
	if task.ErrorMsg != "" {
		status = statusFailure
	}

	if tracked.phase == taskPhasePending {
		// Completed without starting (edge case - maybe no steps)
		asyncOperationsPending.WithLabelValues(operation).Dec()
		asyncOperationPendingDuration.WithLabelValues(operation).Observe(now.Sub(tracked.submittedAt).Seconds())
	} else if tracked.phase == taskPhaseRunning {
		asyncOperationsRunning.WithLabelValues(operation).Dec()
		asyncOperationRunningDuration.WithLabelValues(operation).Observe(now.Sub(tracked.startedAt).Seconds())
	}

	asyncOperationsTotal.WithLabelValues(operation, status).Inc()
	tracked.phase = taskPhaseCompleted

	logFields["status"] = status
	m.logger.WithFields(logFields).Log(m.logLevel, "Task completed")
}

// Cleanup removes a task from internal tracking.
// Call this when the task is deleted from KV store.
func (m *TaskMonitor) Cleanup(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, taskID)
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
		// Unknown task type - don't track metrics
		return ""
	}
}
