package s3

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewMetricsHandler(metricsID *string) block.AdapterMetricsHandler {
	if instance == nil {
		// note that a server can have an instance either with or without 'metrics_id', not both.
		inst := block.InitHistograms("s3", metricsID != nil)
		instance = &inst
	}
	return block.BuildAdapterMetricsHandler(*instance, metricsID)
}
