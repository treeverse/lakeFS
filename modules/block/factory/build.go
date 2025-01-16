package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/stats"
)

func BuildBlockAdapter(ctx context.Context, statsCollector stats.Collector, c params.AdapterConfig) (block.Adapter, error) {
	adapter, err := factory.BuildBlockAdapter(ctx, statsCollector, c)
	if err != nil {
		return nil, err
	}

	return block.NewMetricsAdapter(adapter), nil
}
