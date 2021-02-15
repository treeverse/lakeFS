package testutil

import (
	"fmt"
	"strings"
)

type Histogram struct {
	buckets  []int64
	counters map[int64]int64
}

func NewHistogram(buckets []int64) *Histogram {
	return &Histogram{
		buckets:  buckets,
		counters: make(map[int64]int64),
	}
}

func (h *Histogram) String() string {
	builder := &strings.Builder{}
	for _, b := range h.buckets {
		builder.WriteString(fmt.Sprintf("%d\t%d\n", b, h.counters[b]))
	}
	return builder.String()
}

func (h *Histogram) Add(v int64) {
	for _, b := range h.buckets {
		if v < b {
			h.counters[b]++
		}
	}
}

func (h *Histogram) Clone() *Histogram {
	buckets := make([]int64, len(h.buckets))
	for i, b := range h.buckets {
		buckets[i] = b
	}
	counters := make(map[int64]int64)
	for k, v := range h.counters {
		counters[k] = v
	}
	return &Histogram{
		buckets:  buckets,
		counters: counters,
	}
}
