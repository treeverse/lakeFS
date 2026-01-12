//go:build wireinject
// +build wireinject

package wire

import (
	"context"

	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/icebergsync"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	wireauth "github.com/treeverse/lakefs/pkg/wire/auth"
	wireauthentication "github.com/treeverse/lakefs/pkg/wire/authentication"
	wireblock "github.com/treeverse/lakefs/pkg/wire/block"
	wirecatalog "github.com/treeverse/lakefs/pkg/wire/catalog"
	wireconfig "github.com/treeverse/lakefs/pkg/wire/config"
	wiregateway "github.com/treeverse/lakefs/pkg/wire/gateway"
	wirelicense "github.com/treeverse/lakefs/pkg/wire/license"
	wireapi "github.com/treeverse/lakefs/pkg/wire/api"
)

// InitializeConfig creates the application configuration.
func InitializeConfig(cfgType string) (config.Config, error) {
	wire.Build(wireconfig.ConfigSet)
	return nil, nil
}

// InitializeAuthService creates the auth service with all its dependencies.
func InitializeAuthService(
	ctx context.Context,
	cfg config.Config,
	logger logging.Logger,
	kvStore kv.Store,
) (auth.Service, error) {
	wire.Build(wireauth.AuthSet)
	return nil, nil
}

// InitializeAuthenticationService creates the authentication service.
func InitializeAuthenticationService(
	ctx context.Context,
	cfg config.Config,
	logger logging.Logger,
) (authentication.Service, error) {
	wire.Build(wireauthentication.AuthenticationSet)
	return nil, nil
}

// InitializeAuthenticatorChain creates the authenticator chain.
func InitializeAuthenticatorChain(
	cfg config.Config,
	logger logging.Logger,
	authService auth.Service,
) (auth.ChainAuthenticator, error) {
	wire.Build(wireauthentication.AuthenticationSet)
	return nil, nil
}

// InitializeLoginTokenProvider creates the login token provider.
func InitializeLoginTokenProvider(
	ctx context.Context,
	cfg config.Config,
	logger logging.Logger,
	kvStore kv.Store,
) (authentication.LoginTokenProvider, error) {
	wire.Build(wireauthentication.AuthenticationSet)
	return nil, nil
}

// InitializeBlockAdapter creates the block adapter.
func InitializeBlockAdapter(
	ctx context.Context,
	statsCollector stats.Collector,
	cfg config.Config,
) (block.Adapter, error) {
	wire.Build(wireblock.BlockSet)
	return nil, nil
}

// InitializeLicenseManager creates the license manager.
func InitializeLicenseManager(
	ctx context.Context,
	cfg config.Config,
) (license.Manager, error) {
	wire.Build(wirelicense.LicenseSet)
	return nil, nil
}

// InitializeGatewayMiddleware creates the gateway middleware factory.
func InitializeGatewayMiddleware(
	ctx context.Context,
	cfg config.Config,
	logger logging.Logger,
) (wiregateway.MiddlewareFactory, error) {
	wire.Build(wiregateway.GatewaySet)
	return nil, nil
}

// InitializeIcebergController creates the Iceberg sync controller.
func InitializeIcebergController(cfg config.Config) icebergsync.Controller {
	wire.Build(wireapi.APISet)
	return nil
}

// InitializeConflictResolvers creates the conflict resolvers for merge operations.
func InitializeConflictResolvers(cfg config.Config, blockAdapter block.Adapter) []graveler.ConflictResolver {
	wire.Build(wirecatalog.CatalogSet)
	return nil
}
