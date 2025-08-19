package factory

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

var errSimplifiedOrExternalAuth = errors.New("cannot set auth.ui_config.rbac to non-simplified without setting an external auth service")

func checkAuthModeSupport(baseCfg config.Auth) error {
	if baseCfg.IsAuthBasic() { // Basic mode
		return nil
	}
	if !baseCfg.IsAuthUISimplified() && !baseCfg.IsAuthTypeAPI() {
		return errSimplifiedOrExternalAuth
	}
	return nil
}

func NewAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) auth.Service {
	authCfg := cfg.AuthConfig()
	if err := checkAuthModeSupport(*cfg.AuthConfig()); err != nil {
		logger.WithError(err).Fatal("Unsupported auth mode")
	}

	secretStore := crypt.NewSecretStore([]byte(authCfg.GetBasicAuthConfig().Encrypt.SecretKey))
	if authCfg.IsAuthBasic() {
		apiService := auth.NewBasicAuthService(
			kvStore,
			secretStore,
			authparams.ServiceCache(authCfg.GetBasicAuthConfig().Cache),
			logger.WithField("service", "auth_service"),
		)
		// Check if migration needed
		initialized, err := metadataManager.IsInitialized(ctx)
		if err != nil {
			logger.WithError(err).Fatal("failed to get lakeFS init status")
		}
		if initialized {
			username, err := apiService.Migrate(ctx)
			switch {
			case errors.Is(err, auth.ErrMigrationNotPossible):
				logger.WithError(err).Fatal(`
cannot migrate existing user to basic auth mode!
Please run "lakefs superuser -h" and follow the instructions on how to migrate an existing user
`)
			case err == nil:
				if username != "" { // Print only in case of actual migration
					logger.Infof("\nUser %s was migrated successfully!\n", username)
				}
			default:
				logger.WithError(err).Fatal("basic auth migration failed")
			}
		}
		return auth.NewMonitoredAuthService(apiService)
	}

	// Not Basic - using auth server
	apiService, err := auth.NewAPIAuthService(
		authCfg.GetBasicAuthConfig().API.Endpoint,
		authCfg.GetBasicAuthConfig().API.Token.SecureValue(),
		authCfg.IsAdvancedAuth(),
		authCfg.GetBasicAuthConfig().AuthenticationAPI.ExternalPrincipalsEnabled,
		secretStore,
		authparams.ServiceCache(authCfg.GetBasicAuthConfig().Cache),
		logger.WithField("service", "auth_api"),
	)
	if err != nil {
		logger.WithError(err).Fatal("failed to create authentication service")
	}
	if !authCfg.GetBasicAuthConfig().API.SkipHealthCheck {
		if err := apiService.CheckHealth(ctx, logger, authCfg.GetBasicAuthConfig().API.HealthCheckTimeout); err != nil {
			logger.WithError(err).Fatal("Auth API health check failed")
		}
	}
	return auth.NewMonitoredAuthServiceAndInviter(apiService)
}
