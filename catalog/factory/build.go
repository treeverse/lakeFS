package factory

import (
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

func BuildCataloger(db db.Database, c *config.Config) (catalog.Cataloger, error) {
	if c == nil {
		return mvcc.NewCataloger(db, mvcc.WithCacheEnabled(false)), nil
	}
	catType := c.GetCatalogerType()
	if catType == "rocks" {
		return rocks.NewCataloger(db, c)
	}
	return mvcc.NewCataloger(db, mvcc.WithParams(c.GetMvccCatalogerCatalogParams())), nil
}
