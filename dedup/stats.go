package dedup

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var dedupRemoveObjectDroppedCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "dedup_remove_object_dropped",
		Help: "A counter for dedup remove object that we dropped.",
	},
)

var dedupRemoveObjectFailedCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "dedup_remove_object_failed",
		Help: "A counter for failed dedup remove objects.",
	},
)
