package httputil

import "net/http"

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
