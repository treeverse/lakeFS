package catalog

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	statusSuccess = "success"
	statusFailure = "failure"

	// opUnknown is used for tasks created without the operation type
	opUnknown = "unknown"
)

var (
	asyncOperationsRunning = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_async_operations_running",
			Help: "Number of async operations currently running",
		},
		[]string{"operation"},
	)

	asyncOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lakefs_async_operation_duration_seconds",
			Help:    "Duration of async operations from start to completion",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		},
		[]string{"operation", "status"},
	)

	asyncOperationsExpired = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lakefs_async_operations_expired_total",
			Help: "Total number of async operations that expired without completing",
		},
		[]string{"operation"},
	)
)

// TaskSpan tracks a running async task, recording both Prometheus metrics
// and structured logs for the task lifecycle.
// End() should be deferred to record completion.
type TaskSpan struct {
	log       logging.Logger
	operation string
	task      *Task
	start     time.Time
	ended     bool
}

// StartTaskSpan logs task start, increments in-flight gauge, and returns a span.
// The returned TaskSpan.End() should be deferred to record completion.
func StartTaskSpan(log logging.Logger, task *Task) *TaskSpan {
	operation := task.GetOperation()
	if operation == "" {
		operation = opUnknown
	}

	log = log.WithField("operation", operation)
	asyncOperationsRunning.WithLabelValues(operation).Inc()
	log.Info("Task started")

	return &TaskSpan{
		log:       log,
		operation: operation,
		task:      task,
		start:     time.Now(),
	}
}

// End records task completion metrics and logs. Call via defer.
// It determines success/failure by inspecting the task's StatusCode and ErrorMsg
// fields, so these must be set before End() is called.
// Safe to call multiple times; only the first call has any effect.
func (ts *TaskSpan) End() {
	if ts.ended {
		return
	}
	ts.ended = true

	asyncOperationsRunning.WithLabelValues(ts.operation).Dec()

	status := statusSuccess
	if ts.task.ErrorMsg != "" {
		status = statusFailure
	}

	asyncOperationDuration.WithLabelValues(ts.operation, status).Observe(time.Since(ts.start).Seconds())

	ts.log.WithField("status", status).Info("Task completed")
}

// RecordExpiredTask records metrics and logs for tasks that were cleaned up
// without completing (Done was not set).
func RecordExpiredTask(log logging.Logger, task *Task) {
	operation := task.GetOperation()
	if operation == "" {
		operation = opUnknown
	}

	asyncOperationsExpired.WithLabelValues(operation).Inc()
	log.WithField("operation", operation).Info("Expired task cleaned up")
}
