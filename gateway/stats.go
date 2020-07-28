package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "gateway_requests_duration",
		Help: "request durations in seconds for lakeFS gateway",
	},
	[]string{"operation", "code"})
