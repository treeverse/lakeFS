package httputil

import "net/http"

type MetricResponseWriter struct {
	http.ResponseWriter
	StatusCode int
}

func NewMetricResponseWriter(w http.ResponseWriter) *MetricResponseWriter {
	return &MetricResponseWriter{w, http.StatusOK}
}

func (lrw *MetricResponseWriter) WriteHeader(code int) {
	lrw.StatusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}
