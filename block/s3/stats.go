package s3

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var durationHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "s3_operation_duration_seconds",
		Help: "durations of outgoing s3 operations",
	},
	[]string{"operation", "error"})

var requestSizeHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "s3_operation_size_bytes",
		Help:    "handled sizes of outgoing s3 operations",
		Buckets: prometheus.ExponentialBuckets(1, 10, 10),
	}, []string{"operation", "error"})

func reportMetrics(operation string, start time.Time, sizeBytes *int64, err *error) {
	isErrStr := strconv.FormatBool(*err != nil)
	durationHistograms.WithLabelValues(operation, isErrStr).Observe(time.Since(start).Seconds())
	if sizeBytes != nil {
		requestSizeHistograms.WithLabelValues(operation, isErrStr).Observe(float64(*sizeBytes))
	}
}
