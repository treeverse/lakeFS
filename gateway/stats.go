package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestSummaries = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "gateway_requests_duration",
		Help: "request durations for lakeFS gateway",
	},
	[]string{"operation_id", "code"})
