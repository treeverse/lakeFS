package factory

import (
	"context"

	authhelper "github.com/treeverse/lakefs/modules/auth/helper"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

func NewAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) auth.Service {
	return authhelper.NewAuthService(ctx, cfg, logger, kvStore, metadataManager)
}
