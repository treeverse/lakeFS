package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "gateway_request_duration_seconds",
		Help: "request durations for lakeFS storage gateway",
	},
	[]string{"operation", "code"})
