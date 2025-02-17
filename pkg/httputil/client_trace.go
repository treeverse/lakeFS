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
}, []string{"service", "label"})

func SetClientTrace(ctx context.Context, service string) context.Context {
	return SetClientTraceWithLabel(ctx, service, nil)
}

func SetClientTraceWithLabel(ctx context.Context, service string, label *string) context.Context {
	labelValues := []string{service}
	if label != nil {
		labelValues = append(labelValues, *label)
	}
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			connectionGauge.WithLabelValues(labelValues...).Inc()
		},
		PutIdleConn: func(err error) {
			connectionGauge.WithLabelValues(labelValues...).Dec()
		},
	}

	return httptrace.WithClientTrace(ctx, trace)
}
