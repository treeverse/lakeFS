package httputil

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type MetricResponseWriter struct {
	http.ResponseWriter
	StatusCode int
}

func NewMetricResponseWriter(w http.ResponseWriter) *MetricResponseWriter {
	return &MetricResponseWriter{ResponseWriter: w, StatusCode: http.StatusOK}
}

func (mrw *MetricResponseWriter) WriteHeader(code int) {
	mrw.StatusCode = code
	mrw.ResponseWriter.WriteHeader(code)
}

var ConcurrentRequests = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "lakefs_concurrent_requests",
		Help: "Number of concurrent requests being processed by lakeFS (API and Gateway)",
	},
	[]string{"service", "operation"},
)
