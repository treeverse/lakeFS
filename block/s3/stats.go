package s3

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

var durationHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "s3_adapter_operations_duration",
	},
	[]string{"operation", "error"})

var requestSizeHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "s3_adapter_operations_size",
		Buckets: prometheus.ExponentialBuckets(1, 10, 10),
	}, []string{"operation", "error"})

func reportMetrics(operation string, start time.Time, sizeBytes *int64, err error) {
	isErrStr := fmt.Sprintf("%t", err != nil)
	durationHistograms.WithLabelValues(operation, isErrStr).Observe(time.Since(start).Seconds())
	if sizeBytes != nil {
		requestSizeHistograms.WithLabelValues(operation, isErrStr).Observe(float64(*sizeBytes))
	}
}
