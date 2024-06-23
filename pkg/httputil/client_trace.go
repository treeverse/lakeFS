package httputil

import (
	"net/http"
	"net/http/httptrace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	ServiceContextKey contextKey = "client_trace_service"
)

var connectionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "out_conns",
	Help: "A gauge of in-flight TCP connections",
}, []string{"endpoint", "service"})

func ClientTraceMiddleware(endpoint string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return ClientTraceHandler(endpoint, next)
	}
}

func ClientTraceHandler(endpoint string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var service string
		if service = r.Context().Value(ServiceContextKey).(string); service == "" {
			service = "unknown"
		}

		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				connectionGauge.WithLabelValues(endpoint, service).Inc()
			},
			PutIdleConn: func(err error) {
				connectionGauge.WithLabelValues(endpoint, service).Dec()
			},
		}

		r = r.WithContext(httptrace.WithClientTrace(r.Context(), trace))
		next.ServeHTTP(w, r)
	})
}
