package dedup

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var dedupRemoveObjectFailedCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "dedup_remove_object_failed",
		Help: "A counter for failed dedup remove objects.",
	},
)
