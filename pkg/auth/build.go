package auth

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// NewAuthService returns the auth service, adopting the single stored user of an installation that
// predates basic auth. The service is returned ready to use even when that adoption needs an
// administrator picked by hand, which it reports as ErrMigrationNotPossible so that
// "lakefs superuser" can still run.
func NewAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *KVMetadataManager) (Service, error) {
	baseAuthCfg := cfg.AuthConfig().GetBaseAuthConfig()
	secretStore := crypt.NewSecretStore([]byte(baseAuthCfg.Encrypt.SecretKey))
	apiService := NewBasicAuthService(
		kvStore,
		secretStore,
		authparams.ServiceCache(baseAuthCfg.Cache),
		logger.WithField("service", "auth_service"),
	)
	service := NewMonitoredAuthService(apiService)

	initialized, err := metadataManager.IsInitialized(ctx)
	if err != nil {
		return service, fmt.Errorf("get lakeFS setup state: %w", err)
	}
	if !initialized {
		return service, nil
	}
	username, err := apiService.Migrate(ctx)
	if err != nil {
		return service, err
	}
	if username != "" { // Print only in case of actual migration
		logger.Infof("\nUser %s was migrated successfully!\n", username)
	}
	return service, nil
}
