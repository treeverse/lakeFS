package mvcc

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var dedupBatchSizeHistogram = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "dedup_batch_size",
		Help:    "Dedup batch size histogram",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	},
)

var dedupRemoveObjectDroppedCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "dedup_remove_object_dropped",
		Help: "A counter for dedup remove object that we dropped.",
	},
)
