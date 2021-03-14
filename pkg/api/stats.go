package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		Buckets: []float64{0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 2, 3, 5, 10},
	},
	[]string{"operation", "code"})

func RequestCounterMiddleware() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return promhttp.InstrumentHandlerCounter(requestCounter, next)
	}
}
