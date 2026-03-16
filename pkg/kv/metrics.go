package kv

import (
	"context"
	"runtime/trace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/treeverse/lakefs/pkg/httputil"
)

var (
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_request_duration_seconds",
			Help:    "request durations for the kv Store",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"type", "operation"})

	requestFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_request_failures_total",
		Help: "The total number of errors while working for kv store.",
	}, []string{"type", "operation"})
)

// StoreMetricsWrapper wraps any Store with metrics and traces.
type StoreMetricsWrapper struct {
	Store
	StoreType string
}

func (s *StoreMetricsWrapper) Get(ctx context.Context, partitionKey, key []byte) (*ValueWithPredicate, error) {
	const operation = "Get"
	defer trace.StartRegion(ctx, s.StoreType+":"+operation).End()
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, operation))
	ctx = httputil.SetClientTrace(ctx, s.StoreType)
	defer timer.ObserveDuration()
	res, err := s.Store.Get(ctx, partitionKey, key)
	if err != nil {
		requestFailures.WithLabelValues(s.StoreType, operation).Inc()
	}
	return res, err
}

func (s *StoreMetricsWrapper) Set(ctx context.Context, partitionKey, key, value []byte) error {
	const operation = "Set"
	defer trace.StartRegion(ctx, s.StoreType+":"+operation).End()
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, operation))
	ctx = httputil.SetClientTrace(ctx, s.StoreType)
	defer timer.ObserveDuration()
	err := s.Store.Set(ctx, partitionKey, key, value)
	if err != nil {
		requestFailures.WithLabelValues(s.StoreType, operation).Inc()
	}
	return err
}

func (s *StoreMetricsWrapper) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate Predicate) error {
	const operation = "SetIf"
	defer trace.StartRegion(ctx, s.StoreType+":"+operation).End()
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, operation))
	ctx = httputil.SetClientTrace(ctx, s.StoreType)
	defer timer.ObserveDuration()
	err := s.Store.SetIf(ctx, partitionKey, key, value, valuePredicate)
	if err != nil {
		requestFailures.WithLabelValues(s.StoreType, operation).Inc()
	}
	return err
}

func (s *StoreMetricsWrapper) Delete(ctx context.Context, partitionKey, key []byte) error {
	const operation = "Delete"
	defer trace.StartRegion(ctx, s.StoreType+":"+operation).End()
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, operation))
	ctx = httputil.SetClientTrace(ctx, s.StoreType)
	defer timer.ObserveDuration()
	err := s.Store.Delete(ctx, partitionKey, key)
	if err != nil {
		requestFailures.WithLabelValues(s.StoreType, operation).Inc()
	}
	return err
}

func (s *StoreMetricsWrapper) Scan(ctx context.Context, partitionKey []byte, options ScanOptions) (EntriesIterator, error) {
	const operation = "Scan"
	defer trace.StartRegion(ctx, s.StoreType+":"+operation+":start").End()
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, operation))
	ctx = httputil.SetClientTrace(ctx, s.StoreType)
	defer timer.ObserveDuration()
	res, err := s.Store.Scan(ctx, partitionKey, options)
	if err != nil {
		requestFailures.WithLabelValues(s.StoreType, operation).Inc()
		return nil, err
	}
	return &tracingEntriesIterator{ctx: ctx, iter: res, wrapper: s}, nil
}

func (s *StoreMetricsWrapper) Close() {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "Close"))
	defer timer.ObserveDuration()
	s.Store.Close()
}

func storeMetrics(store Store, storeType string) *StoreMetricsWrapper {
	return &StoreMetricsWrapper{Store: store, StoreType: storeType}
}

// tracingEntriesIterator wraps EntriesIterator with tracing.
type tracingEntriesIterator struct {
	ctx     context.Context
	iter    EntriesIterator
	wrapper *StoreMetricsWrapper
}

func (t *tracingEntriesIterator) Next() bool {
	defer trace.StartRegion(t.ctx, t.wrapper.StoreType+":Scan:next").End()
	return t.iter.Next()
}

func (t *tracingEntriesIterator) SeekGE(key []byte) {
	t.iter.SeekGE(key)
}

func (t *tracingEntriesIterator) Entry() *Entry {
	return t.iter.Entry()
}

func (t *tracingEntriesIterator) Err() error {
	return t.iter.Err()
}

func (t *tracingEntriesIterator) Close() {
	defer trace.StartRegion(t.ctx, t.wrapper.StoreType+":Scan:end").End()
	t.iter.Close()
}
