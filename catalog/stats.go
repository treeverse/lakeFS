package catalog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var batchSizeCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "create_entry_batch_size",
		Help: "A counter for create entry batch size.",
	},
	[]string{"size"},
)
