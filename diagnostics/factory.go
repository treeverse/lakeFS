package diagnostics

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

type Collector interface {
	Collect(ctx context.Context, w io.Writer) (err error)
}

func CreateCollector(adb db.Database, cataloger catalog.Cataloger, cfg *config.Config, adapter block.Adapter) Collector {
	if cataloger == nil {
		return NewDBCollector(adb)
	}
	return NewBlockCollector(adb, cataloger, cfg, adapter)
}
