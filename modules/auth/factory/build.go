package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	authfactoryhelper "github.com/treeverse/lakefs/pkg/auth/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

func NewAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) (auth.Service, error) {
	return authfactoryhelper.NewAuthService(ctx, cfg, logger, kvStore, metadataManager), nil
}
