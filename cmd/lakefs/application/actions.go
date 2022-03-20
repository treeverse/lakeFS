package application

import (
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/catalog"
)

func NewActionsService(cmd LakeFsCmd, dbService *DatabaseService, c *catalog.Catalog, blockStore *BlockStore) *actions.Service {
	actionsService := actions.NewService(
		cmd.ctx,
		dbService.dbPool,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		blockStore.bufferedCollector,
		cmd.cfg.GetActionsEnabled(),
	)
	c.SetHooksHandler(actionsService)
	return actionsService
}
