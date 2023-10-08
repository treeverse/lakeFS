package dynamodb

import (
	"context"
)

type NopRateLimiter struct{}

func (r *NopRateLimiter) GetToken(_ context.Context, _ uint) (releaseToken func() error, err error) {
	return func() error { return nil }, nil
}

func (r *NopRateLimiter) AddTokens(uint) error {
	return nil
}
