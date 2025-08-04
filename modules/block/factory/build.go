package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/stats"
)

func BuildBlockAdapter(ctx context.Context, statsCollector stats.Collector, c config.Config, serviceName ...string) (block.Adapter, error) {
	adapter, err := factory.BuildBlockAdapter(ctx, statsCollector, c.StorageConfig().GetStorageByID(config.SingleBlockstoreID))
	if err != nil {
		return nil, err
	}

	service := "unknown"
	if len(serviceName) > 0 && serviceName[0] != "" {
		service = serviceName[0]
	}

	return block.NewMetricsAdapter(adapter, service), nil
}
