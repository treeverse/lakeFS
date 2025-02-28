package azure

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewMetricsHandler(adapterStatsID *string) block.AdapterMetricsHandler {
	if instance == nil {
		// note that a server can have an instance either with or without 'adapter_stats_id', not both.
		inst := block.InitHistograms("azure", adapterStatsID != nil)
		instance = &inst
	}
	return block.BuildAdapterMetricsHandler(*instance, adapterStatsID)
}
