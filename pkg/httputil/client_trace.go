package httputil

import (
	"context"
	"net/http/httptrace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var connectionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "out_conns",
	Help: "A gauge of in-use TCP connections",
}, []string{"service"})

func SetClientTrace(ctx context.Context, service string) context.Context {
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			connectionGauge.WithLabelValues(service).Inc()
		},
		PutIdleConn: func(err error) {
			connectionGauge.WithLabelValues(service).Dec()
		},
	}

	return httptrace.WithClientTrace(ctx, trace)
}
