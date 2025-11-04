package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

func NewSyncManager(_ context.Context, _ config.Config) (icebergcatalog.SyncManager, error) {
	return &icebergcatalog.NopSyncManager{}, nil
}
