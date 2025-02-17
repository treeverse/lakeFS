package azure

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Stats struct {
	durationHistograms    *prometheus.HistogramVec
	requestSizeHistograms *prometheus.HistogramVec
	tag                   *string
}

var durationHistograms *prometheus.HistogramVec
var requestSizeHistograms *prometheus.HistogramVec

func NewAzureStats(tag *string) *Stats {
	labelNames := []string{"operation", "error"}
	if tag != nil {
		labelNames = append(labelNames, "tag")
	}
	if durationHistograms == nil {
		durationHistograms = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "azure_operation_duration_seconds",
				Help: "durations of outgoing azure operations",
			},
			labelNames,
		)
	}
	if requestSizeHistograms == nil {
		requestSizeHistograms = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "azure_operation_size_bytes",
				Help:    "handled sizes of outgoing azure operations",
				Buckets: prometheus.ExponentialBuckets(1, 10, 10), //nolint: mnd
			},
			labelNames,
		)
	}

	return &Stats{
		durationHistograms:    durationHistograms,
		requestSizeHistograms: requestSizeHistograms,
	}
}

func (s Stats) reportMetrics(operation string, start time.Time, sizeBytes *int64, err *error) {
	isErrStr := strconv.FormatBool(*err != nil)
	labels := []string{operation, isErrStr}
	if s.tag != nil {
		labels = append(labels, *s.tag)
	}
	s.durationHistograms.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	if sizeBytes != nil {
		s.requestSizeHistograms.WithLabelValues(labels...).Observe(float64(*sizeBytes))
	}
}
