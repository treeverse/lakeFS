package factory

import (
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

func NewSyncController(_ config.Config) icebergcatalog.SyncController {
	return &icebergcatalog.NopSyncController{}
}
