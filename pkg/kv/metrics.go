package kv

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestHistograms = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "kv_request_duration_seconds",
		Help:    "request durations for the kv store",
		Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	},
	[]string{"type", "operation"})

// storeMetricsWrapper wraps any Store with metrics
type storeMetricsWrapper struct {
	store     Store
	storeType string
}

func (s *storeMetricsWrapper) wrapWithMetrics(op string, f func()) {
	start := time.Now()
	f()
	requestHistograms.WithLabelValues(s.storeType, op).Observe(time.Since(start).Seconds())
}

func (s *storeMetricsWrapper) Get(ctx context.Context, partitionKey, key []byte) (*ValueWithPredicate, error) {
	var res *ValueWithPredicate
	var err error

	s.wrapWithMetrics("Get", func() {
		res, err = s.store.Get(ctx, partitionKey, key)
	})
	return res, err
}

func (s *storeMetricsWrapper) Set(ctx context.Context, partitionKey, key, value []byte) error {
	var err error

	s.wrapWithMetrics("Set", func() {
		err = s.store.Set(ctx, partitionKey, key, value)
	})
	return err
}

func (s *storeMetricsWrapper) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate Predicate) error {
	var err error

	s.wrapWithMetrics("SetIf", func() {
		err = s.store.SetIf(ctx, partitionKey, key, value, valuePredicate)
	})
	return err
}

func (s *storeMetricsWrapper) Delete(ctx context.Context, partitionKey, key []byte) error {
	var err error

	s.wrapWithMetrics("Delete", func() {
		err = s.store.Delete(ctx, partitionKey, key)
	})
	return err
}

func (s *storeMetricsWrapper) Scan(ctx context.Context, partitionKey, start []byte) (EntriesIterator, error) {
	var res EntriesIterator
	var err error

	s.wrapWithMetrics("Scan", func() {
		res, err = s.store.Scan(ctx, partitionKey, start)
	})
	return res, err
}

func (s *storeMetricsWrapper) Close() {
	s.wrapWithMetrics("Close", func() {
		s.store.Close()
	})
}

func newStoreMetricsWrapper(store Store, storeType string) *storeMetricsWrapper {
	return &storeMetricsWrapper{store: store, storeType: storeType}
}
