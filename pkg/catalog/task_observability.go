package catalog

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	statusSuccess  = "success"
	statusFailure  = "failure"
	statusExpired  = "expired"  // Task completed but exceeded client-facing deadline
	statusOrphaned = "orphaned" // Task heartbeat stopped and was cleaned up

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
	if task == nil {
		return &TaskSpan{}
	}

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
	if ts.task == nil || ts.ended {
		return
	}
	ts.ended = true

	asyncOperationsRunning.WithLabelValues(ts.operation).Dec()

	status := statusSuccess
	if ts.task.ErrorMsg != "" {
		status = statusFailure
	} else if ts.task.StatusCode == http.StatusRequestTimeout {
		status = statusExpired
	}

	asyncOperationsTotal.WithLabelValues(ts.operation, status).Inc()
	asyncOperationDuration.WithLabelValues(ts.operation).Observe(time.Since(ts.start).Seconds())

	ts.log.WithField("status", status).Info("Task completed")
}

// RecordOrphanedTask records metrics and logs for tasks that were cleaned up
// due to stopped heartbeat.
func RecordOrphanedTask(log logging.Logger, task *Task) {
	if task == nil {
		return
	}

	operation := task.GetOperation()
	if operation == "" {
		operation = opUnknown
	}

	asyncOperationsTotal.WithLabelValues(operation, statusOrphaned).Inc()
	log.WithFields(logging.Fields{
		"operation": operation,
		"status":    statusOrphaned,
	}).Info("Orphaned task cleaned up")
}
