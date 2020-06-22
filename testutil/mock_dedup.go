package testutil

import "context"

type MockDedup struct {
	DedupIndex map[string]string
}

func NewMockDedup() *MockDedup {
	m := make(map[string]string)
	return &MockDedup{DedupIndex: m}
}

func (d *MockDedup) Dedup(_ context.Context, _ string, dedupID string, addr string) (string, error) {
	existingObj, ok := d.DedupIndex[dedupID]
	if ok {
		return existingObj, nil
	} else {
		d.DedupIndex[dedupID] = addr
		return addr, nil
	}
}
