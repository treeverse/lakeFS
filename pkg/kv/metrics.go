package kv

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "kv_request_duration_seconds",
		Help:    "request durations for the kv Store",
		Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.25, 0.5, 1, 2.5, 5, 10},
	},
	[]string{"type", "operation"})

// StoreMetricsWrapper wraps any Store with metrics
type StoreMetricsWrapper struct {
	Store
	StoreType string
}

func (s *StoreMetricsWrapper) Get(ctx context.Context, partitionKey, key []byte) (*ValueWithPredicate, error) {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "Get"))
	defer timer.ObserveDuration()
	return s.Store.Get(ctx, partitionKey, key)
}

func (s *StoreMetricsWrapper) Set(ctx context.Context, partitionKey, key, value []byte) error {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "Set"))
	defer timer.ObserveDuration()
	return s.Store.Set(ctx, partitionKey, key, value)
}

func (s *StoreMetricsWrapper) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate Predicate) error {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "SetIf"))
	defer timer.ObserveDuration()
	return s.Store.SetIf(ctx, partitionKey, key, value, valuePredicate)
}

func (s *StoreMetricsWrapper) Delete(ctx context.Context, partitionKey, key []byte) error {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "Delete"))
	defer timer.ObserveDuration()
	return s.Store.Delete(ctx, partitionKey, key)
}

func (s *StoreMetricsWrapper) Scan(ctx context.Context, partitionKey []byte, options ScanOptions) (EntriesIterator, error) {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "Scan"))
	defer timer.ObserveDuration()
	return s.Store.Scan(ctx, partitionKey, options)
}

func (s *StoreMetricsWrapper) Close() {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(s.StoreType, "Close"))
	defer timer.ObserveDuration()
	s.Store.Close()
}

func storeMetrics(store Store, storeType string) *StoreMetricsWrapper {
	return &StoreMetricsWrapper{Store: store, StoreType: storeType}
}
