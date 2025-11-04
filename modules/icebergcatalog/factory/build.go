package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

func NewSyncManager(_ config.Config) icebergcatalog.SyncManager {
	return &icebergcatalog.NopSyncManager{}
}
