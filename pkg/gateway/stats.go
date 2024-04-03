package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "gateway_request_duration_seconds",
		Help:    "request durations for lakeFS storage gateway",
		Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60},
	},
	[]string{"operation", "code"})
