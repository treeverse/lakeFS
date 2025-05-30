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
		Name:    "api_request_duration_seconds",
		Help:    "request durations for lakeFS API",
		Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60},
	},
	[]string{"operation", "code"})

var requestTTFBHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "api_request_ttfb_duration_seconds",
		Help:    "request time-to-first-byte durations for lakeFS API",
		Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60},
	},
	[]string{"operation"})
