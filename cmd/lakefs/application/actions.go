package application

import (
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/stats"
)

func NewActionsService(cmdCtx LakeFsCmdContext, dbService *DatabaseService, c *catalog.Catalog, bufferedCollector *stats.BufferedCollector) *actions.Service {
	actionsService := actions.NewService(
		cmdCtx.ctx,
		dbService.dbPool,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		bufferedCollector,
		cmdCtx.cfg.GetActionsEnabled(),
	)
	c.SetHooksHandler(actionsService)
	return actionsService
}
