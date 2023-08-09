package kv

import (
	"context"

	"go.uber.org/ratelimit"
)

type StoreLimiter struct {
	Store   Store
	Limiter ratelimit.Limiter
}

func NewStoreLimiter(s Store, l ratelimit.Limiter) *StoreLimiter {
	return &StoreLimiter{
		Store:   s,
		Limiter: l,
	}
}

func (s *StoreLimiter) Get(ctx context.Context, partitionKey, key []byte) (*ValueWithPredicate, error) {
	_ = s.Limiter.Take()
	return s.Store.Get(ctx, partitionKey, key)
}

func (s *StoreLimiter) Set(ctx context.Context, partitionKey, key, value []byte) error {
	_ = s.Limiter.Take()
	return s.Store.Set(ctx, partitionKey, key, value)
}

func (s *StoreLimiter) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate Predicate) error {
	_ = s.Limiter.Take()
	return s.Store.SetIf(ctx, partitionKey, key, value, valuePredicate)
}

func (s *StoreLimiter) Delete(ctx context.Context, partitionKey, key []byte) error {
	_ = s.Limiter.Take()
	return s.Store.Delete(ctx, partitionKey, key)
}

func (s *StoreLimiter) Scan(ctx context.Context, partitionKey []byte, options ScanOptions) (EntriesIterator, error) {
	_ = s.Limiter.Take()
	return s.Store.Scan(ctx, partitionKey, options)
}

func (s *StoreLimiter) Close() {
	s.Store.Close()
}
