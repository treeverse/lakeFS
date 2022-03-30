package application

import (
	"context"

	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/stats"
)

func NewActionsService(ctx context.Context, cmdCtx LakeFsCmdContext, dbService *DatabaseService, c *catalog.Catalog, bufferedCollector *stats.BufferedCollector) *actions.Service {
	actionsService := actions.NewService(
		ctx,
		dbService.dbPool,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		bufferedCollector,
		cmdCtx.cfg.GetActionsEnabled(),
	)
	c.SetHooksHandler(actionsService)
	return actionsService
}
