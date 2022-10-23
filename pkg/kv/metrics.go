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
		Help:    "request durations for the kv Store",
		Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	},
	[]string{"type", "operation"})

// StoreMetricsWrapper wraps any Store with metrics
type StoreMetricsWrapper struct {
	Store     Store
	storeType string
}

func (s *StoreMetricsWrapper) wrapWithMetrics(op string, f func()) {
	start := time.Now()
	f()
	requestHistograms.WithLabelValues(s.storeType, op).Observe(time.Since(start).Seconds())
}

func (s *StoreMetricsWrapper) Get(ctx context.Context, partitionKey, key []byte) (*ValueWithPredicate, error) {
	var res *ValueWithPredicate
	var err error

	s.wrapWithMetrics("Get", func() {
		res, err = s.Store.Get(ctx, partitionKey, key)
	})
	return res, err
}

func (s *StoreMetricsWrapper) Set(ctx context.Context, partitionKey, key, value []byte) error {
	var err error

	s.wrapWithMetrics("Set", func() {
		err = s.Store.Set(ctx, partitionKey, key, value)
	})
	return err
}

func (s *StoreMetricsWrapper) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate Predicate) error {
	var err error

	s.wrapWithMetrics("SetIf", func() {
		err = s.Store.SetIf(ctx, partitionKey, key, value, valuePredicate)
	})
	return err
}

func (s *StoreMetricsWrapper) Delete(ctx context.Context, partitionKey, key []byte) error {
	var err error

	s.wrapWithMetrics("Delete", func() {
		err = s.Store.Delete(ctx, partitionKey, key)
	})
	return err
}

func (s *StoreMetricsWrapper) Scan(ctx context.Context, partitionKey, start []byte) (EntriesIterator, error) {
	var res EntriesIterator
	var err error

	s.wrapWithMetrics("Scan", func() {
		res, err = s.Store.Scan(ctx, partitionKey, start)
	})
	return res, err
}

func (s *StoreMetricsWrapper) Close() {
	s.wrapWithMetrics("Close", func() {
		s.Store.Close()
	})
}

func newStoreMetricsWrapper(store Store, storeType string) *StoreMetricsWrapper {
	return &StoreMetricsWrapper{Store: store, storeType: storeType}
}
