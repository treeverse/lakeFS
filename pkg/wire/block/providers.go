package block

import (
	"context"

	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/stats"
)

// BlockSet provides block adapter creation.
// External projects can replace this set to provide custom block adapter implementations.
var BlockSet = wire.NewSet(
	BuildBlockAdapter,
)

// BuildBlockAdapter creates the block adapter based on configuration.
// It wraps the adapter with metrics collection.
func BuildBlockAdapter(ctx context.Context, statsCollector stats.Collector, c config.Config) (block.Adapter, error) {
	adapter, err := factory.BuildBlockAdapter(ctx, statsCollector, c.StorageConfig().GetStorageByID(config.SingleBlockstoreID))
	if err != nil {
		return nil, err
	}

	return block.NewMetricsAdapter(adapter), nil
}
