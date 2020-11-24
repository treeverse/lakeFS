package factory

import (
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

func BuildCatalog(c *config.Config, db db.Database) catalog.Cataloger {
	catType := c.GetCatalogerType()
	if catType == "rocks" {
		return rocks.NewCataloger()
	}
	return mvcc.NewCataloger(db, mvcc.WithParams(c.GetMvccCatalogerCatalogParams()))
}
