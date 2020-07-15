package catalog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var dedupBatchSizeCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "dedup_batch_size_count",
		Help: "A counter for each batch size used by dedup",
	},
	[]string{"size"},
)
