package httputil

import (
	"context"
	"net/http/httptrace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Known limitation: for HTTP2 services, like cosmosDB,
// the gauge is never reduced. Hence, we'll treat it as a counter for new connections created.
var connectionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "out_in_use_conns",
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
