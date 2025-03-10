package azure

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var durationHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "azure_operation_duration_seconds",
		Help: "durations of outgoing azure operations",
	},
	[]string{"operation", "storage_id", "error"})

var requestSizeHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "azure_operation_size_bytes",
		Help:    "handled sizes of outgoing azure operations",
		Buckets: prometheus.ExponentialBuckets(1, 10, 10), //nolint: mnd
	}, []string{"operation", "storage_id", "error"})

func reportMetrics(operation, storageID string, start time.Time, sizeBytes *int64, err *error) {
	isErrStr := strconv.FormatBool(*err != nil)
	durationHistograms.WithLabelValues(operation, storageID, isErrStr).Observe(time.Since(start).Seconds())
	if sizeBytes != nil {
		requestSizeHistograms.WithLabelValues(operation, storageID, isErrStr).Observe(float64(*sizeBytes))
	}
}
