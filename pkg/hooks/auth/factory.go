package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	authfactoryhelper "github.com/treeverse/lakefs/pkg/auth/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/entensionhooks"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// NewAuthService returns the OSS auth service or an enterprise override.
func NewAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) (auth.Service, error) {
	if hook := entensionhooks.Get().AuthService; hook != nil {
		return hook.New(ctx, cfg, logger, kvStore, metadataManager)
	}
	return authfactoryhelper.NewAuthService(ctx, cfg, logger, kvStore, metadataManager), nil
}
