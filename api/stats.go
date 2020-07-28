package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "api_requests_total",
		Help: "A counter for requests to the wrapped handler.",
	},
	[]string{"code", "method"},
)

var requestSummaries = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "api_requests_duration",
		Help: "request durations for lakeFS API",
	},
	[]string{"operation_id", "code"})
