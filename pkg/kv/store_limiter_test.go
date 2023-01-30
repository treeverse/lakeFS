package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/mock"
)

type traceLimiter struct {
	Count int
}

func (t *traceLimiter) Take() time.Time {
	t.Count++
	return time.Now()
}

// TestStoreLimiter verify limiter is used for all kv store operations
func TestStoreLimiter(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	store := mock.NewMockStore(ctrl)

	rec := store.EXPECT()
	rec.Scan(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
	rec.Get(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
	rec.Delete(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
	rec.Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
	rec.SetIf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
	rec.Close().Times(1)

	limiter := &traceLimiter{}
	sl := kv.NewStoreLimiter(store, limiter)
	defer sl.Close()
	_, _ = sl.Scan(ctx, nil, kv.ScanOptions{})
	_, _ = sl.Get(ctx, nil, nil)
	_ = sl.Delete(ctx, nil, nil)
	_ = sl.Set(ctx, nil, nil, nil)
	_ = sl.SetIf(ctx, nil, nil, nil, nil)

	const expectedCalls = 5
	if expectedCalls != limiter.Count {
		t.Fatalf("Limiter called=%d, expected=%d", limiter.Count, expectedCalls)
	}
}
