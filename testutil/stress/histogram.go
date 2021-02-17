package stress

import (
	"fmt"
	"strings"
)

type Histogram struct {
	buckets  []int64
	counters map[int64]int64
	min      int64
	max      int64
	total    int64
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
	builder.WriteString(fmt.Sprintf("min\t%d\n", h.min))
	builder.WriteString(fmt.Sprintf("max\t%d\n", h.max))
	builder.WriteString(fmt.Sprintf("total\t%d\n", h.total))
	return builder.String()
}

func (h *Histogram) Add(v int64) {
	if v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.total++
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
		min:      h.min,
		max:      h.max,
		total:    h.total,
	}
}
