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

var requestHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "api_request_duration_seconds",
		Help: "request durations for lakeFS API",
	},
	[]string{"operation", "code"})
