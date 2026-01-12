package auth

import (
	"context"
	"errors"

	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// AuthSet provides auth service creation.
// External projects can replace this set to provide custom auth implementations.
var AuthSet = wire.NewSet(
	NewAuthService,
	NewMetadataManager,
)

var errSimplifiedOrExternalAuth = errors.New("cannot set auth.ui_config.rbac to non-simplified without setting an external auth service")

func checkAuthModeSupport(authCfg config.AuthConfig) error {
	baseAuthCfg := authCfg.GetBaseAuthConfig()
	authUICfg := authCfg.GetAuthUIConfig()

	if authUICfg.IsAuthBasic() { // Basic mode
		return nil
	}
	if !authUICfg.IsAuthUISimplified() && !baseAuthCfg.IsAuthTypeAPI() {
		return errSimplifiedOrExternalAuth
	}
	return nil
}

// NewMetadataManager creates the auth metadata manager.
func NewMetadataManager(kvStore kv.Store, cfg config.Config) *auth.KVMetadataManager {
	baseCfg := cfg.GetBaseConfig()
	return auth.NewKVMetadataManager("", "", baseCfg.Database.Type, kvStore)
}

// NewAuthService creates the auth service based on configuration.
func NewAuthService(
	ctx context.Context,
	cfg config.Config,
	logger logging.Logger,
	kvStore kv.Store,
	metadataManager *auth.KVMetadataManager,
) (auth.Service, error) {
	authCfg := cfg.AuthConfig()
	baseAuthCfg := authCfg.GetBaseAuthConfig()
	authUICfg := authCfg.GetAuthUIConfig()
	if err := checkAuthModeSupport(authCfg); err != nil {
		return nil, err
	}

	secretStore := crypt.NewSecretStore([]byte(baseAuthCfg.Encrypt.SecretKey))
	if authUICfg.IsAuthBasic() {
		apiService := auth.NewBasicAuthService(
			kvStore,
			secretStore,
			authparams.ServiceCache(baseAuthCfg.Cache),
			logger.WithField("service", "auth_service"),
		)
		// Check if migration needed
		initialized, err := metadataManager.IsInitialized(ctx)
		if err != nil {
			return nil, err
		}
		if initialized {
			username, err := apiService.Migrate(ctx)
			switch {
			case errors.Is(err, auth.ErrMigrationNotPossible):
				return nil, errors.New("cannot migrate existing user to basic auth mode - run 'lakefs superuser -h' for migration instructions")
			case err == nil:
				if username != "" {
					logger.Infof("\nUser %s was migrated successfully!\n", username)
				}
			default:
				return nil, err
			}
		}
		return auth.NewMonitoredAuthService(apiService), nil
	}

	// Not Basic - using auth server
	apiService, err := auth.NewAPIAuthService(
		baseAuthCfg.API.Endpoint,
		baseAuthCfg.API.Token.SecureValue(),
		authUICfg.IsAdvancedAuth(),
		baseAuthCfg.AuthenticationAPI.ExternalPrincipalsEnabled,
		secretStore,
		authparams.ServiceCache(baseAuthCfg.Cache),
		logger.WithField("service", "auth_api"),
	)
	if err != nil {
		return nil, err
	}
	if !baseAuthCfg.API.SkipHealthCheck {
		if err := apiService.CheckHealth(ctx, logger, baseAuthCfg.API.HealthCheckTimeout); err != nil {
			return nil, err
		}
	}
	return auth.NewMonitoredAuthServiceAndInviter(apiService), nil
}
