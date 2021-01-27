package factory

import (
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

func BuildCataloger(db db.Database, c *config.Config) (catalog.Cataloger, error) {
	return rocks.NewCataloger(db, c)
}
